using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using kafka4net.ConsumerImpl;
using kafka4net.Tracing;
using kafka4net.Utils;

namespace kafka4net
{
    public class Consumer : IDisposable
    {
        public readonly ConsumerState State = new ConsumerState();

        internal ConsumerConfiguration Configuration { get; private set; }
        internal string Topic { get { return Configuration.Topic; } }

        private readonly Cluster _cluster;

        // Track map of partition ID to TopicPartition as well as partition ID to the IDisposable subscription for that TopicPartition.
        private readonly Dictionary<int, TopicPartition> _topicPartitions = new Dictionary<int, TopicPartition>(); 
        private readonly Dictionary<int, IDisposable> _partitionsSubscription = new Dictionary<int, IDisposable>();

        private ITargetBlock<ReceivedMessage> _consumerTarget;

        private bool _isDisposed;
        private static readonly ILogger _log = Logger.GetLogger();
        static readonly Task COMPLETE_TASK;

        static Consumer()
        {
            var t = new TaskCompletionSource<bool>();
            t.SetResult(true);
            COMPLETE_TASK = t.Task;
        }

        /// <summary>
        /// Create a new consumer using the specified configuration. See @ConsumerConfiguration
        /// </summary>
        public Consumer(ConsumerConfiguration consumerConfig, ITargetBlock<ReceivedMessage> consumerTarget)
        {
            Configuration = consumerConfig;
            _consumerTarget = consumerTarget;
            _cluster = new Cluster(consumerConfig.SeedBrokers);
            _cluster.OnThreadHang += e => _consumerTarget.Fault(e);

            // handle stop condition
            //onMessage = onMessage.Do(message =>
            //{
            //    // check if this partition is done per the condition passed in configuration. If so, unsubscribe it.
            //    bool partitionDone = (Configuration.StopPosition.IsPartitionConsumingComplete(message));
            //    IDisposable partitionSubscription;
            //    if (partitionDone && _partitionsSubscription.TryGetValue(message.Partition, out partitionSubscription))
            //    {
            //        _partitionsSubscription.Remove(message.Partition);
            //        // calling Dispose here will cause the OnTopicPartitionComplete method to be called when it is completed.
            //        partitionSubscription.Dispose();
            //    }
            //});

            // If permanent error within any single partition, fail the whole consumer (intentionally). 
            // Do not try to keep going (failfast principle).
            _cluster.PartitionStateChanges.
                Where(s => s.ErrorCode.IsPermanentFailure()).
                Subscribe(state =>
                {
                    Dispose();
                    _consumerTarget.Fault(new PartitionFailedException(state.Topic, state.PartitionId, state.ErrorCode));
                });
        }

        public async Task ConnectAsync()
        {
            try
            {
                await _cluster.ConnectAsync();
                await SubscribeClient();
                EtwTrace.Log.ConsumerStarted(GetHashCode(), Topic);

                // check that we actually got any partitions subscribed
                if (_topicPartitions.Count == 0)
                    _consumerTarget.Complete();

                State._connected.TrySetResult(true);
            }
            catch (Exception e)
            {
                _consumerTarget.Fault(e);
                
                State._connected.TrySetException(e);
                State._closed.TrySetException(e);
            }
        }

        /// <summary>
        /// Start listening to partitions.
        /// </summary>
        /// <returns>Cleanup disposable which unsubscribes from partitions and clears the _topicPartitions</returns>
        async Task SubscribeClient()
        {
            await await _cluster.Scheduler.Ask(async () =>
            {
                // subscribe to all partitions
                var parts = await BuildTopicPartitionsAsync();
                parts.ForEach(part =>
                {
                    var partSubscription = part.Subscribe(this);
                    _partitionsSubscription.Add(part.PartitionId,partSubscription);
                });
                _log.Debug("Subscribed to partitions");
                return true;
            });
        }

        internal async Task OnPartitionMessage(ReceivedMessage[] batch)
        {
            foreach (var msg in batch)
                await _consumerTarget.SendAsync(msg);

            //
            // Handle stop condition
            //
            foreach (var msg in batch)
            {
                // check if this partition is done per the condition passed in configuration. If so, unsubscribe it.
                bool partitionDone = (Configuration.StopPosition.IsPartitionConsumingComplete(msg));
                IDisposable partitionSubscription;
                if (partitionDone && _partitionsSubscription.TryGetValue(msg.Partition, out partitionSubscription))
                {
                    _partitionsSubscription.Remove(msg.Partition);
                    // calling Dispose here will cause the OnTopicPartitionComplete method to be called when it is completed.
                    partitionSubscription.Dispose();
                    
                    if(_partitionsSubscription.Count == 0)
                        _consumerTarget.Complete();
                }

            }
        }

        /// <summary>
        ///  Provides access to the Cluster for this consumer
        /// </summary>
        public Cluster Cluster { get { return _cluster; } }

        /// <summary>For every patition, resolve offsets and build TopicPartition object</summary>
        private async Task<IEnumerable<TopicPartition>> BuildTopicPartitionsAsync()
        {
            // if they didn't specify explicit locations, initialize them here.
            var startPositionProvider = Configuration.StartPosition;
            if (startPositionProvider.StartLocation != ConsumerLocation.SpecifiedLocations)
            {
                // no offsets provided. Need to issue an offset request to get start/end locations and use them for consuming
                var partitions = await _cluster.FetchPartitionOffsetsAsync(Topic, startPositionProvider.StartLocation);

                if (_log.IsDebugEnabled)
                    _log.Debug("Consumer for topic {0} got time->offset resolved for location {1}. parts: [{2}]",
                        Topic, startPositionProvider,
                        string.Join(",", partitions.Partitions.OrderBy(p => p).Select(p => string.Format("{0}:{1}", p, partitions.NextOffset(p)))));

                IStartPositionProvider origProvider = startPositionProvider;
                // the new explicit offsets provider should use only the partitions included in the original one.
                startPositionProvider = new TopicPartitionOffsets(partitions.Topic, partitions.GetPartitionsOffset.Where(kv=> origProvider.ShouldConsumePartition(kv.Key)));
            }

            // we now have specified locations to start from, just get the partition metadata, and build the TopicPartitions
            var partitionMeta = await _cluster.GetOrFetchMetaForTopicAsync(Topic);
            return partitionMeta
                // only new partitions we don't already have in our dictionary
                .Where(pm => !_topicPartitions.ContainsKey(pm.Id))
                // only partitions we are "told" to.
                .Where(pm => startPositionProvider.ShouldConsumePartition(pm.Id))
                .Select(part =>
                {
                    var tp = new TopicPartition(_cluster, Topic, part.Id, startPositionProvider.GetStartOffset(part.Id));
                    _topicPartitions.Add(tp.PartitionId, tp);
                    return tp;
                });
        }

        public override string ToString()
        {
            return string.Format("Consumer: '{0}'", Topic);
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            try
            {
                _partitionsSubscription.Values.ForEach(s=>s.Dispose());
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch {}

            var clusteShutdownTask = COMPLETE_TASK;
            try
            {
                // close and release the connections in the Cluster.
                if (_cluster != null && _cluster.State != Cluster.ClusterState.Disconnected)
                    clusteShutdownTask = _cluster.CloseAsync(TimeSpan.FromSeconds(5)).
                        ContinueWith(t => _log.Error(t.Exception, "Error when closing Cluster"), TaskContinuationOptions.OnlyOnFaulted);
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch(Exception e)
            {
                _log.Error(e, "Error in Dispose");
            }

            _consumerTarget.Complete();

            clusteShutdownTask.ContinueWith(_ => 
            {
                State._connected.TrySetResult(false);
                State._closed.TrySetResult(true);
            });

            _isDisposed = true;
        }

        public void MessageHandler(Action<ReceivedMessage> action, int buffer = 1000, int degreeOfParallelism = 1)
        {
            _consumerTarget = new ActionBlock<ReceivedMessage>(action, new ExecutionDataflowBlockOptions { BoundedCapacity = buffer, MaxDegreeOfParallelism = degreeOfParallelism });
        }

        public void MessageHandler(Func<ReceivedMessage,Task> action, int buffer = 1000, int degreeOfParallelism = 1)
        {
            _consumerTarget = new ActionBlock<ReceivedMessage>(action, new ExecutionDataflowBlockOptions { BoundedCapacity = buffer, MaxDegreeOfParallelism = degreeOfParallelism });
        }
    }
}
