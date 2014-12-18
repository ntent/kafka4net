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
        private readonly SemaphoreSlim _sync = new SemaphoreSlim(1,1);

        // cancellation token used to notify all producer components to stop.
        private readonly CancellationTokenSource _shutdown = new CancellationTokenSource();
        private readonly CancellationTokenSource _drain = new CancellationTokenSource();

        // the _allPartitionQueues dictionary contains queues for all of the "PartitionBatches" indexed by topic/partition. 
        // the sending loop will send out of these queues as quickly as possible, however when there is an issue with 
        // sending, these queues will back up. Note that it ispossible when some partitions work and some are recovering abd backed up.
        // TODO: the Queue in here needs to eventually be wrapped with logic to have size bounds
        private readonly Dictionary<int,PartitionQueueInfo> _allPartitionQueues = new Dictionary<int, PartitionQueueInfo>();
        private readonly ManualResetEventSlim _queueEventWaitHandler = new ManualResetEventSlim(false, 0);

        // Producer ID (unique number for each Producer instance. used in debugging messages.)
        private static int _idCount;
        private readonly int _id = Interlocked.Increment(ref _idCount);

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
                await _sync.WaitAsync();

                try
                {
                    if (IsConnected)
                        return;

                    _log.Debug("Connecting producer {1} for topic {0}", Topic, _id);
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
                                queue = new PartitionQueueInfo(Configuration.SendBuffersInitialSize) { Partition = p.PartitionId };
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
                                    queue = new PartitionQueueInfo(Configuration.SendBuffersInitialSize) { Partition = batch.Key };
                                    _allPartitionQueues.Add(batch.Key, queue);
                                    queue.IsOnline = topicPartitions.First(p => p.Id == batch.Key).ErrorCode == ErrorCode.NoError;
                                    _log.Debug("{0} added new partition queue", this);
                                }

                                // now queue them up
                                var batchAr = batch.ToArray();

                                // make sure we have space.
                                if (queue.Queue.Size + batchAr.Length > queue.Queue.Capacity)
                                {
                                    // try to increase the capacity.
                                    if (Configuration.AutoGrowSendBuffers)
                                    {
                                        var growBy = Math.Max(queue.Queue.Capacity/2 + 1, 2 * batchAr.Length);
                                        if (_log.IsDebugEnabled)
                                            _log.Debug("Capacity of send buffer with size {3} not large enough to accept {2} new messages. Increasing capacity from {0} to {1}", queue.Queue.Capacity, queue.Queue.Capacity+growBy, batchAr.Length, queue.Queue.Size);

                                        queue.Queue.Capacity += growBy;
                                    }
                                    else
                                    {
                                        // we're full and not allowed to grow. Throw the batch back to the caller, and continue on.
                                        if (OnPermError != null)
                                            OnPermError(
                                                new Exception(string.Format("Send Buffer Full for partition {0}. ",
                                                    Cluster.PartitionStateChanges.Where(
                                                        ps => ps.Topic == Configuration.Topic && ps.PartitionId == queue.Partition)
                                                        .Take(1)
                                                        .Wait()))
                                                , batchAr);
                                        continue;
                                    }
                                }

                                // we have the space, add to the queue
                                queue.Queue.Put(batchAr);

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
                }
                catch (Exception e)
                {
                    _log.Error(e, "Exception during connect");
					throw;
                }
                finally
                {
                    _log.Debug("#{0} Releasing Producer Semaphore.", _id);
                    _sync.Release();
                }
            }).ConfigureAwait(false);
        }

        public void Send(Message msg)
        {
            if (_shutdown.IsCancellationRequested || _drain.IsCancellationRequested)
                throw new Exception("Cannot send messages after producer is canceled / closed.");

            if (!IsConnected)
                throw new Exception("Must call ConnectAsync prior to sending messages.");

            _sendMessagesSubject.OnNext(msg);

        }

        public async Task CloseAsync(TimeSpan timeout)
        {
            await _sync.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!IsConnected)
                    return;

                _log.Debug("Closing...");
                // mark the cancellation token to cause the sending to finish up and don't allow any new messages coming in.
                _drain.Cancel();
            
                // flush messages to partition queues
                _log.Debug("Completing _sendMessagesSubject...");
                _sendMessagesSubject.OnCompleted();
                _log.Debug("Completed _sendMessagesSubject");

                // trigger send loop into action, if it is waiting on empty queue
                _queueEventWaitHandler.Set();
            
                // wait for sending to complete 
                _log.Debug("Waiting for Send buffers to drain...");
                if(_sendLoopTask != null)
                    if (await _sendLoopTask.TimeoutAfter(timeout).ConfigureAwait(false))
                        _log.Debug("Send loop completed");
                    else
                    {
                        _log.Error("Timed out while waiting for Send buffers to drain. Canceling.");
                        _shutdown.Cancel();
                        if (!await _sendLoopTask.TimeoutAfter(timeout).ConfigureAwait(false))
                            _log.Fatal("Timed out even after cancelling send loop! This shouldn't happen, there will likely be message loss.");
                    }

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

                _sendMessagesSubject = null;
                _log.Debug("Close complete");
            }
            finally
            {
                _sync.Release();
            }
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
                    if (_drain.IsCancellationRequested && _allPartitionQueues.Values.All(q => q.Queue.Size == 0))
                    {
                        _log.Debug("Cancel detected and send buffers are empty. Quitting SendLoop");
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

                // if we are being told to shut down (already waited for drain), then send all messages back over OnPermError, and quit.
                if (_shutdown.IsCancellationRequested)
                {
                    var messages = _allPartitionQueues.SelectMany(pq=>pq.Value.Queue.Get(pq.Value.Queue.Size)).ToArray();
                    if (messages.Length > 0)
                    {
                        var msg = string.Format("Not all messages could be sent before shutdown. {0} messages remain.", messages.Length);
                        if (OnPermError != null)
                        {
                            OnPermError(new Exception(msg), messages);
                            _log.Error(msg);
                        }
                        else
                        {
                            _log.Fatal("{0} There is no OnPermError handler so these messages are LOST!", msg);
                        }
                    }

                    break;
                }

                var queuesToBeSent = _allPartitionQueues.Values.
                    Where(q => q.IsReadyForServing).
                    ToArray();

                if (_log.IsDebugEnabled)
                    _log.Debug("There are {0} partition queues with {1} total messages to send.", queuesToBeSent.Length, queuesToBeSent.Sum(qi=>qi.Queue.Size));

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
                        Select(queues => new { queues.Leader, queues.Queues }).
                        Select(async brokerBatch =>
                        {
                            try
                            {
                                var maxMessages = Configuration.MaxMessagesPerProduceRequest;
                                var totalPending = brokerBatch.Queues.Sum(q => q.Queue.Size);

                                if (_log.IsDebugEnabled)
                                    _log.Debug("Sending {0} of {1} total messages pending for broker {2}.", Math.Min(maxMessages,totalPending), totalPending, brokerBatch.Leader);

                                // traverse pending queues, taking what we can, and tracking "extra" to give to other partitions.
                                // we will take all if we can, otherwise take only up to our "fair" share.
                                // if there are less than maxMessages to be sent to this broker, just take all of them.
                                var messages = new Message[Math.Min(maxMessages,totalPending)];
                                var totalTaken = 0;
                                var ration = maxMessages/brokerBatch.Queues.Length;
                                var carryover = 0;
                                var currentBatch = 0;

                                brokerBatch.Queues.OrderBy(q=>q.Queue.Size).ForEach(q =>
                                {
                                    // take the lesser of our total size or the amount rationed to us.
                                    var take = Math.Min(ration , q.Queue.Size);

                                    // if we don't need our full ration, give it to the big guys
                                    if (take < ration)
                                        carryover += ration - take;
                                    else if (carryover > 0)
                                    {
                                        // this queue is over his ration. reset the carryover and re-calculate the ration.
                                        ration = (messages.Length-totalTaken)/(brokerBatch.Queues.Length-currentBatch);
                                        carryover = 0;
                                    }

                                    // track how many we're taking 
                                    q.CountInProgress = take;
                                    q.Queue.Peek(messages, totalTaken, take);
                                    totalTaken += take;
                                    currentBatch++;
                                });


                                if (messages.Take(totalTaken).Any(m=>m==null))
                                    _log.Error("Null Messages Detected!");

                                // TODO: freeze permanent errors and throw consumer exceptions upon sending to perm error partition
                                var response = await _cluster.SendBatchAsync(brokerBatch.Leader, messages.Take(totalPending), this);

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
                                
                                var successMessages = successPartitionQueues.SelectMany(q => q.Queue.Get(q.CountInProgress)).ToArray();

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
            public PartitionQueueInfo(int maxBufferSize)
            {
                Queue = new CircularBuffer<Message>(maxBufferSize); // TODO: Max Enqueued Batches configuration!
            }

            public readonly CircularBuffer<Message> Queue;
            public int Partition;
            public bool InProgress;
            public int CountInProgress;
            public bool IsOnline;

            public bool IsReadyForServing { get { return !InProgress && IsOnline && Queue.Size > 0; } }
        }
    }
}
