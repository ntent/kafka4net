using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using kafka4net.Utils;

namespace kafka4net
{
    public class Producer
    {
        private static readonly ILogger _log = Logger.GetLogger();

        public readonly ProducerConfiguration Configuration;
        public string Topic { get { return Configuration.Topic; } }
        public Cluster Cluster { get { return _cluster; } }

        public Action<Message[]> OnTempError;
        public Action<Exception, Message[]> OnPermError;
        public Action<Message[]> OnShutdownDirty;
        public Action<Message[]> OnSuccess;

        //public MessageCodec Codec = MessageCodec.CodecNone;
        private Task _sendLoopTask;
        private readonly Subject<Message> _sendMessagesSubject;
        private readonly Cluster _cluster;

        // cancellation token used to notify all producer components to stop.
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

        // the _allPartitionQueues dictionary contains queues for all of the "PartitionBatches" indexed by topic/partition. 
        // the sending loop will send out of these queues as quickly as possible, however when there is an issue with 
        // sending, these queues will back up. Note that it ispossible when some partitions work and some are recovering abd backed up.
        // TODO: the Queue in here needs to eventually be wrapped with logic to have size bounds
        private readonly Dictionary<int,PartitionQueueInfo> _allPartitionQueues = new Dictionary<int, PartitionQueueInfo>();

        public Producer(ProducerConfiguration producerConfiguration)
        {
            Configuration = producerConfiguration;
            _cluster = new Cluster(producerConfiguration.SeedBrokers);
            _sendMessagesSubject = new Subject<Message>();
        }

        public async Task ConnectAsync()
        {
            await await _cluster.Scheduler.Ask(async () =>
            {
                await _cluster.ConnectAsync();
                // get all of the partitions for this topic. Allows the MessagePartitioner to select a partition.
                var topicPartitions = await _cluster.GetOrFetchMetaForTopicAsync(Configuration.Topic);
                _log.Debug("Producer found {0} partitions for '{1}'", topicPartitions.Length, Configuration.Topic);

                _sendMessagesSubject.
                    ObserveOn(_cluster.Scheduler).
                    Do(msg => msg.PartitionId = Configuration.Partitioner.GetMessagePartition(msg, topicPartitions).Id).
                    GroupBy(m => m.PartitionId).
                    Subscribe(part =>
                    {
                        var queue = new PartitionQueueInfo { Partition = part.Key };
                        lock (_allPartitionQueues)
                        {
                            _allPartitionQueues.Add(part.Key, queue);
                        }

                        part.
                            // TODO: buffer incoming messages, because configuration means incoming characteristics?
                            Buffer(Configuration.BatchFlushTime, Configuration.BatchFlushSize).
                            Synchronize(_allPartitionQueues).
                            Do(batch =>
                            {
                                queue.Queue.Enqueue(batch.ToArray());
                                if (_log.IsDebugEnabled)
                                    _log.Debug("Enqueued batch of size {0} for topic '{1}' partition {2}", batch.Count, Configuration.Topic, part.Key);
                            }).
                            // After batch enqueued, send wake up sygnal to sending queue
                            Subscribe(_ =>
                                {
                                    lock (_allPartitionQueues)
                                        Monitor.Pulse(_allPartitionQueues);
                                },
                                e => _log.Error(e, "Error in batch pipeline"),
                                () => _log.Debug("Batch pipeline complete")
                            );

                        _log.Debug("{0} added new partition queue", this);
                    }, e => _log.Fatal(e, "Error in message processing pipeline"),
                    () => _log.Debug("Message processing pipeline complete")
                );

                // start the send loop task
                _sendLoopTask = Task.Run((Func<Task>) SendLoop);

                _sendLoopTask.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                        _log.Fatal(t.Exception, "SendLoop failed");
                    else
                        _log.Debug("SendLoop {0}", t.Status);
                });
            });
        }

        public void Send(Message msg)
        {
            if (_cancellation.IsCancellationRequested)
                throw new Exception("Cannot send messages after producer is canceled / closed.");
                
            _sendMessagesSubject.OnNext(msg);

        }

        public async Task Close(TimeSpan timeout)
        {
            _log.Debug("Closing...");
            // mark the cancellation token to cause the sending to finish up and don't allow any new messages coming in.
            _cancellation.Cancel();
            
            // complete the incoming message stream
            _sendMessagesSubject.OnCompleted();

            // trigger send loop to exit
            lock(_allPartitionQueues)
            {
                Monitor.Pulse(_allPartitionQueues);
            }
            
            // wait for sending to complete 
            await _sendLoopTask.ConfigureAwait(false);

            // close down the cluster
            await _cluster.CloseAsync(timeout);
            _log.Info("Close complete");
        }

        async Task SendLoop()
        {
            _log.Debug("Starting SendLoop for {0}", this);
            var partsMeta = await _cluster.GetOrFetchMetaForTopicAsync(Configuration.Topic);

            while (true)
            {
                PartitionQueueInfo[] queuesToBeSent;
                lock(_allPartitionQueues)
                {
                    if (_allPartitionQueues.Values.All(q => q.InProgress || q.Queue.Count == 0))
                    {
                        if (_cancellation.Token.IsCancellationRequested)
                        {
                            _log.Debug("Cancel detected. Quitting SendLoop");
                            break;
                        }
                        _log.Debug("Waiting for queue event {0}", this);
                        Monitor.Wait(_allPartitionQueues);
                        _log.Debug("Got queue event {0}", this);
                    }
                    else
                    {
                        if(_log.IsDebugEnabled)
                            _log.Debug("There are batches in queues, continue working");
                    }

                    queuesToBeSent = _allPartitionQueues.Values.
                        Where(q => !q.InProgress && q.Queue.Count != 0).
                        ToArray();

                    if(queuesToBeSent.Length == 0)
                        continue;

                    // while sill in lock, mark queues as in-progress to skip sending in next iteration
                    queuesToBeSent.ForEach(q => q.InProgress = true);
                    _log.Debug("Locked queues: [{0}]", string.Join(",", queuesToBeSent.Select(q => q.Partition)));
                }

                //
                // Do sending outside of _allPartitionQueues lock
                //

                queuesToBeSent.
                    Select(q => new {partsMeta.First(m => m.Id == q.Partition).Leader, Queue = q}).
                    // leader,queue -> leader,queue[]
                    GroupBy(q => q.Leader, (i, queues) => new {Leader = i, Queues = queues.Select(q1 => q1.Queue).ToArray()}).
                    Select(queues => new { queues.Leader, Messages = queues.Queues.SelectMany(q => q.Queue.Peek()) }).
                    ForEach(async brokerBatch =>
                    {
                        try
                        {
                            // TODO: should retry period be different?
                            // TODO: freeze permanent errors and throw consumer exceptions upon sending to perm error partition
                            var result = await _cluster.SendBatchAsync(brokerBatch.Leader, brokerBatch.Messages, this);
                            if (result.Topics.All(t => t.Partitions.All(p => p.ErrorCode == ErrorCode.NoError)))
                            {
                                // no errors, dismiss batches from queues
                                lock (_allPartitionQueues)
                                {
                                    var messageCount = queuesToBeSent.Sum(q => q.Queue.Dequeue().Length);
                                    _log.Debug("Sent successfully and dequeued {0} messages, topic '{1}' brokerId {2}", messageCount, Configuration.Topic, brokerBatch.Leader);
                                }
                            }
                            else
                            {
                                // some errors, figure out which batches to dismiss from queues
                                var failedPartitions = result.Topics.SelectMany(t => t.Partitions).Where(p => p.ErrorCode != ErrorCode.NoError).ToArray();
                                var messageCount = queuesToBeSent.
                                    Where(q => failedPartitions.All(f => f.Partition != q.Partition)).
                                    Sum(q => q.Queue.Dequeue().Length);
                                if (_log.IsDebugEnabled)
                                {
                                    var errorList = failedPartitions.Select(p => string.Format("{0}->{1}", p.Partition, p.ErrorCode));
                                    _log.Debug("Sent successfuly and dequeued {0} but there were errors: topic: '{1}' [{2}]",
                                        messageCount, Configuration.Topic, string.Join(" ", errorList));
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            _log.Debug(e, "Exception while sending batch to topic '{0}' BrokerId {1}", Configuration.Topic, brokerBatch.Leader);
                        }
                        finally
                        {
                            lock(_allPartitionQueues)
                            {
                                queuesToBeSent.ForEach(q => q.InProgress = false);
                                // there might be more batches in unlocked queues, trigger event to check it
                                Monitor.Pulse(_allPartitionQueues);
                            }
                        }
                    });

            }
        }

        public override string ToString()
        {
            return string.Format("'{0}' Batch flush time: {1} Batch flush size: {2}", Topic, Configuration.BatchFlushTime, Configuration.BatchFlushSize);
        }

        class PartitionQueueInfo
        {
            public readonly Queue<Message[]> Queue = new Queue<Message[]>();
            public int Partition;
            public bool InProgress;
        }
    }
}
