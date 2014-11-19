using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Internal;
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
        private Subject<Message> _sendMessagesSubject;
        private readonly Cluster _cluster;
        private readonly bool _internalCluster;

        // cancellation token used to notify all producer components to stop.
        private readonly CancellationTokenSource _shutdown = new CancellationTokenSource();

        // the _allPartitionQueues dictionary contains queues for all of the "PartitionBatches" indexed by topic/partition. 
        // the sending loop will send out of these queues as quickly as possible, however when there is an issue with 
        // sending, these queues will back up. Note that it ispossible when some partitions work and some are recovering abd backed up.
        // TODO: the Queue in here needs to eventually be wrapped with logic to have size bounds
        private readonly Dictionary<int,PartitionQueueInfo> _allPartitionQueues = new Dictionary<int, PartitionQueueInfo>();
        ManualResetEventSlim _queueEventWaitHandler = new ManualResetEventSlim(false, 0);

        public Producer(Cluster cluster, ProducerConfiguration producerConfiguration)
        {
            Configuration = producerConfiguration;
            _cluster = cluster;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="seedBrokers">Comma separated list of seed brokers. Port numbers are optional.
        /// <example>192.168.56.10,192.168.56.20:8081,broker3.local.net:8181</example>
        /// </param>
        /// <param name="producerConfiguration"></param>
        public Producer(string seedBrokers, ProducerConfiguration producerConfiguration)
            : this(new Cluster(seedBrokers), producerConfiguration)
        {
            _internalCluster = true;
        }


        public bool IsConnected { get { return _sendMessagesSubject != null; } }

        public async Task ConnectAsync()
        {
            // if we've already connected, don't do it again.
            if (IsConnected)
                return;

            await await _cluster.Scheduler.Ask(async () =>
            {
                _log.Debug("Connecting");
                _sendMessagesSubject = new Subject<Message>();

                if (_cluster.State != Cluster.ClusterState.Connected)
                {
                    _log.Debug("Connecting cluster");
                    await _cluster.ConnectAsync();
                    _log.Debug("Connected cluster");
                }

                // Recovery: subscribe to partition offline/online events
                _cluster.PartitionStateChanges.
                    Where(p => p.Topic == Configuration.Topic).
                    Synchronize(_allPartitionQueues).
                    Subscribe(p =>
                    {
                        PartitionQueueInfo queue;
                        if (!_allPartitionQueues.TryGetValue(p.PartitionId, out queue))
                        {
                            queue = new PartitionQueueInfo {Partition = p.PartitionId};
                            _allPartitionQueues.Add(p.PartitionId, queue);
                        }

                        if (_log.IsDebugEnabled)
                        {
                            _log.Debug("Changing '{0}'/{1} IsOnline {2}->{3}", Configuration.Topic, p.PartitionId, queue.IsOnline, p.ErrorCode == ErrorCode.NoError);
                        }

                        queue.IsOnline = p.ErrorCode == ErrorCode.NoError;
                        _queueEventWaitHandler.Set();

                        if (_log.IsDebugEnabled)
                        {
                            _log.Debug("Detected change in topic/partition '{0}'/{1}/{2}. Triggered queue event", Configuration.Topic, p.PartitionId, p.ErrorCode);
                        }
                    });

                // get all of the partitions for this topic. Allows the MessagePartitioner to select a partition.
                var topicPartitions = await _cluster.GetOrFetchMetaForTopicAsync(Configuration.Topic);
                _log.Debug("Producer found {0} partitions for '{1}'", topicPartitions.Length, Configuration.Topic);

                _sendMessagesSubject.
                    ObserveOn(_cluster.Scheduler).
                    Do(msg => msg.PartitionId = Configuration.Partitioner.GetMessagePartition(msg, topicPartitions).Id).
                    Buffer(Configuration.BatchFlushTime, Configuration.BatchFlushSize).
                    Where(b => b.Count > 0).
                    Select(msgs => msgs.GroupBy(msg => msg.PartitionId)).
                    ObserveOn(_cluster.Scheduler).
                    Subscribe(partitionGroups =>
                    {
                        foreach (var batch in partitionGroups)
                        {
                            // partition queue might be created if metadata broadcast fired already
                            PartitionQueueInfo queue;
                            if (!_allPartitionQueues.TryGetValue(batch.Key, out queue))
                            {
                                queue = new PartitionQueueInfo {Partition = batch.Key};
                                _allPartitionQueues.Add(batch.Key, queue);
                                queue.IsOnline = topicPartitions.First(p => p.Id == batch.Key).ErrorCode ==
                                                    ErrorCode.NoError;
                                _log.Debug("{0} added new partition queue", this);
                            }

                            // now go through each group and queue them up
                            var batchAr = batch.ToArray();
                            queue.Queue.Enqueue(batchAr);
                            if (_log.IsDebugEnabled)
                                _log.Debug("Enqueued batch of size {0} for topic '{1}' partition {2}",
                                    batchAr.Length, Configuration.Topic, batch.Key);

                            // After batch enqueued, send wake up signal to sending queue
                            _queueEventWaitHandler.Set();
                        }
                    }, e => _log.Fatal(e, "Error in _sendMessagesSubject pipeline"),
                    () => _log.Debug("_sendMessagesSubject complete")
                );

                // start the send loop task
                _cluster.Scheduler.Schedule(() => {
                    _sendLoopTask = SendLoop().
                    ContinueWith(t =>
                    {
                        if (t.IsFaulted)
                            _log.Fatal(t.Exception, "SendLoop failed");
                        else
                            _log.Debug("SendLoop complete with status: {0}", t.Status);
                    });
                });
                _log.Debug("Connected");
            }).ConfigureAwait(false);
        }

        public void Send(Message msg)
        {
            if (_shutdown.IsCancellationRequested)
                throw new Exception("Cannot send messages after producer is canceled / closed.");

            if (!IsConnected)
                throw new Exception("Must call ConnectAsync prior to sending messages.");

            _sendMessagesSubject.OnNext(msg);

        }

        public async Task Close(TimeSpan timeout)
        {
            _log.Debug("Closing...");
            // mark the cancellation token to cause the sending to finish up and don't allow any new messages coming in.
            _shutdown.Cancel();
            
            // flush messages to partition queues
            _log.Debug("Completing _sendMessagesSubject...");
            _sendMessagesSubject.OnCompleted();
            _log.Debug("Completed _sendMessagesSubject");

            // trigger send loop into action, if it is waiting on empty queue
            _queueEventWaitHandler.Set();
            
            // wait for sending to complete 
            _log.Debug("Waiting for send loop complete...");
            if(_sendLoopTask != null)
                if(await _sendLoopTask.TimeoutAfter(timeout).ConfigureAwait(false))
                    _log.Debug("Send loop completed");
                else
                    _log.Error("Timed out while waiting for _sendLoop");

            // close down the cluster ONLY if we created it
            if (_internalCluster)
            {
                _log.Debug("Closing internal cluster...");
                await _cluster.CloseAsync(timeout).ConfigureAwait(false);
            }
            else
            {
                _log.Debug("Not closing external shared cluster.");
            }

            _log.Debug("Close complete");
        }

        private async Task SendLoop()
        {
            _log.Debug("Starting SendLoop for {0}", this);

            while (true)
            {
                var partsMeta = await await _cluster.Scheduler.Ask(() => _cluster.GetOrFetchMetaForTopicAsync(Configuration.Topic));
                
                if (_allPartitionQueues.Values.All(q => !q.IsReadyForServing))
                {
                    // TODO: bug: if messages are accumulating in buffer, this will quit. Wait for buffer drainage
                    if (_shutdown.Token.IsCancellationRequested && _allPartitionQueues.Values.All(q => q.Queue.Count == 0))
                    {
                        _log.Debug("Cancel detected. Quitting SendLoop");
                        break;
                    }
                    
                    _log.Debug("Waiting for queue event {0}", Configuration.Topic);
                    await Task.Run(() => 
                    { 
                        _log.Debug("Start waiting in a thread");
                        _queueEventWaitHandler.Wait();
                        _queueEventWaitHandler.Reset();
                    });
                    
                    _log.Debug("Got queue event {0}", Configuration.Topic);
                }
                else
                {
                    if(_log.IsDebugEnabled)
                        _log.Debug("There are batches in queues, continue working");
                }

                var queuesToBeSent = _allPartitionQueues.Values.
                    Where(q => q.IsReadyForServing).
                    ToArray();

                _log.Debug("queuesToBeSent.Length {0}", queuesToBeSent.Length);

                if(queuesToBeSent.Length == 0)
                    continue;

                // while sill in lock, mark queues as in-progress to skip sending in next iteration
                _log.Debug("Locking queues: '{0}'/[{1}]", Configuration.Topic, string.Join(",", queuesToBeSent.Select(q => q.Partition)));
                queuesToBeSent.ForEach(q => q.InProgress = true);

                //
                // Send ProduceRequist
                //
                try
                {
                    var sendTasks = queuesToBeSent.
                        Select(q => new { partsMeta.First(m => m.Id == q.Partition).Leader, Queue = q }).
                        // leader,queue -> leader,queue[]
                        GroupBy(q => q.Leader, (i, queues) => new { Leader = i, Queues = queues.Select(q1 => q1.Queue).ToArray() }).
                        Select(queues => new { queues.Leader, queues.Queues, Messages = queues.Queues.SelectMany(q => q.Queue.Peek()) }).
                        Select(async brokerBatch =>
                        {
                            try
                            {
                                // TODO: freeze permanent errors and throw consumer exceptions upon sending to perm error partition
                                var response = await _cluster.SendBatchAsync(brokerBatch.Leader, brokerBatch.Messages, this);

                                var failedResponsePartitions = response.Topics.
                                    Where(t => t.TopicName == Configuration.Topic). // should contain response only for our topic, but just in case...
                                    SelectMany(t => t.Partitions).
                                    Where(p => p.ErrorCode != ErrorCode.NoError).ToList();

                                // some errors, figure out which batches to dismiss from queues
                                var failedPartitionIds = new HashSet<int>(failedResponsePartitions.Select(p => p.Partition));

                                var successPartitionQueues = brokerBatch.Queues.
                                    Where(q => !failedPartitionIds.Contains(q.Partition)).
                                    ToArray();

                                // notify of any errors from send response
                                failedResponsePartitions.ForEach(failedPart => 
                                    _cluster.NotifyPartitionStateChange(new PartitionStateChangeEvent(Topic, failedPart.Partition, failedPart.ErrorCode)));


                                var successMessages = successPartitionQueues.SelectMany(q => q.Queue.Dequeue()).ToArray();

                                if (OnSuccess != null && successMessages.Length != 0)
                                    OnSuccess(successMessages);
                            }
                            catch (Exception e)
                            {
                                _log.Debug(e, "Exception while sending batch to topic '{0}' BrokerId {1}", Configuration.Topic, brokerBatch.Leader);
                                //brokerBatch.Queues.ForEach(partQueue => _cluster.NotifyPartitionStateChange(new Tuple<string, int, ErrorCode>(Topic, partQueue.Partition, ErrorCode.TransportError)));
                            }
                        });

                    await Task.WhenAll(sendTasks);
                }
                finally
                {
                    queuesToBeSent.ForEach(q => q.InProgress = false);

                    if (_log.IsDebugEnabled)
                        _log.Debug("Unlocked queue '{0}'/{1}", Configuration.Topic, string.Join(",", queuesToBeSent.Select(q => q.Partition)));
                }
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
            public bool IsOnline;

            public bool IsReadyForServing { get { return !InProgress && IsOnline && Queue.Count > 0; } }
        }
    }
}
