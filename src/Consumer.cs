using System.Linq;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Threading;
using kafka4net.ConsumerImpl;
using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using kafka4net.Tracing;
using kafka4net.Utils;

namespace kafka4net
{
    public class Consumer : IDisposable
    {
        public readonly IObservable<ReceivedMessage> OnMessageArrived;
        internal readonly ISubject<ReceivedMessage> OnMessageArrivedInput = new Subject<ReceivedMessage>();

        private static readonly ILogger _log = Logger.GetLogger();

        internal ConsumerConfiguration Configuration { get; private set; }
        internal string Topic { get { return Configuration.Topic; } }

        private readonly Cluster _cluster;
        private readonly Dictionary<int, TopicPartition> _topicPartitions = new Dictionary<int, TopicPartition>(); 
        // we should only ever subscribe once, and we want to keep that around to check if it was disposed when we are disposed
        private readonly CompositeDisposable _partitionsSubscription = new CompositeDisposable();
        readonly TaskCompletionSource<bool> _connectionComplete = new TaskCompletionSource<bool>();

        int _outstandingMessageProcessingCount;
        int _haveSubscriber;

        public bool FlowControlEnabled { get; private set; }

        readonly BehaviorSubject<int> _flowControlInput = new BehaviorSubject<int>(1);
        // Is used by Fetchers to wake up from sleep when flow turns ON
        internal IObservable<bool> FlowControl;


        /// <summary>
        /// Create a new consumer using the specified configuration. See @ConsumerConfiguration
        /// </summary>
        /// <param name="consumerConfig"></param>
        public Consumer(ConsumerConfiguration consumerConfig)
        {
            Configuration = consumerConfig;
            _cluster = new Cluster(consumerConfig.SeedBrokers);

            // Low/high watermark implementation
            FlowControl = _flowControlInput.
                Scan(1, (last, current) =>
                {
                    if (current < Configuration.LowWatermark)
                        return 1;
                    if (current > Configuration.HighWatermark)
                        return -1;
                    return last; // While in between watermarks, carry over previous on/off state
                }).
                    DistinctUntilChanged().
                    Select(i => i > 0).
                    Do(f =>FlowControlEnabled = f).
                    Do(f => _log.Debug("Flow control '{0}' on {1}", f ? "Open" : "Closed", this));

            var onMessage = Observable.Create<ReceivedMessage>(observer =>
            {
                if (Interlocked.CompareExchange(ref _haveSubscriber, 1, 0) == 1)
                    throw new InvalidOperationException("Only one subscriber is allowed. Use OnMessageArrived.Publish().RefCount()");

                // Relay messages from partition to consumer's output
                // Ensure that only single subscriber is allowed because it is important to count
                // outstanding messaged consumed by user
                OnMessageArrivedInput.Subscribe(observer);

                //
                // It is not possible to wait for completion of partition resolution process, so start it asynchronously.
                // This means that OnMessageArrived.Subscribe() will complete when consumer is not actually connected yet.
                //
                Task.Run(() => _cluster.Scheduler.Ask(async () =>
                {
                    try 
                    {
                        await _cluster.ConnectAsync();
                        await SubscribeClient();
                        EtwTrace.Log.ConsumerStarted(GetHashCode(), Topic);
                        _connectionComplete.TrySetResult(true);
                    }
                    catch (Exception e)
                    {
                        _connectionComplete.TrySetException(e);
                        observer.OnError(e);
                    }
                }));

                // upon unsubscribe
                return Disposable.Create(() =>
                {
                    _partitionsSubscription.Dispose();
                    _topicPartitions.Clear();
                });
            });

            if (Configuration.UseFlowControl) { 
            // Increment counter of messages sent for processing
                onMessage = onMessage.Do(msg =>
                {
                    var count = Interlocked.Increment(ref _outstandingMessageProcessingCount);
                    _flowControlInput.OnNext(count);
                });
            }

            // Propagate exceptions in subscribe directly to caller by using Scheduler.Immediate
            onMessage = onMessage.SubscribeOn(Scheduler.Immediate);

            // Isolate driver's scheduler thread from the caller because driver's scheduler is sensitive for delays
            var scheduledMessages = SynchronizationContext.Current != null ? onMessage.ObserveOn(SynchronizationContext.Current) : onMessage.ObserveOn(Scheduler.Default);

            OnMessageArrived = scheduledMessages;
        }

        /// <summary>
        /// Start listening to partitions.
        /// </summary>
        /// <returns>Cleanup disposable which unsubscribes from partitions and clears the _topicPartitions</returns>
        async Task SubscribeClient()
        {
            var val = await await _cluster.Scheduler.Ask(async () =>
            {
                // subscribe to all partitions
                var parts = await BuildTopicPartitionsAsync();
                parts.ForEach(part =>
                {
                    var partSubscription = part.Subscribe(this);
                    _partitionsSubscription.Add(partSubscription);
                });
                _log.Debug("Subscribed to partitions");
                return true;
            });
        }

        public void Ack(int messageCount = 1)
        {
            if(!Configuration.UseFlowControl)
                throw new InvalidOperationException("UseFlowControl is OFF, Ack is not allowed");

            var count = Interlocked.Add(ref _outstandingMessageProcessingCount, - messageCount);
            _flowControlInput.OnNext(count);
        }

        /// <summary>
        /// Pass connection method through from Cluster
        /// </summary>
        /// <returns></returns>
        public Task IsConnected 
        {
            get 
            {
                if(_haveSubscriber == 0)
                    throw new BrokerException("Can not connect because no subscription yet");
                return _connectionComplete.Task;
            }
        }

        /// <summary>
        ///  Provides access to the Cluster for this consumer
        /// </summary>
        public Cluster Cluster { get { return _cluster; } }

        /// <summary>For every patition, resolve offsets and build TopicPartition object</summary>
        private async Task<IEnumerable<TopicPartition>> BuildTopicPartitionsAsync()
        {
            // two code paths here. If we have specified locations to start from, just get the partition metadata, and build the TopicPartitions
            if (Configuration.StartLocation == ConsumerStartLocation.SpecifiedLocations)
            {
                var partitionMeta = await _cluster.GetOrFetchMetaForTopicAsync(Topic);
                return partitionMeta
                    .Where(pm => !_topicPartitions.ContainsKey(pm.Id))
                    .Select(part =>
                    {
                        var tp = new TopicPartition(_cluster, Topic, part.Id, Configuration.PartitionOffsetProvider(part.Id));
                        _topicPartitions.Add(tp.PartitionId, tp);
                        return tp;
                    });
            }

            // no offsets provided. Need to issue an offset request to get start/end locations and use them for consuming
            var partitions = await _cluster.FetchPartitionOffsetsAsync(Topic, Configuration.StartLocation);

            if (_log.IsDebugEnabled)
                _log.Debug("Consumer for topic {0} got time->offset resolved for location {1}. parts: [{2}]", 
                    Topic, Configuration.StartLocation, 
                    string.Join(",", partitions.Partitions.OrderBy(p=>p).Select(p => string.Format("{0}:{1}", p, partitions.NextOffset(p)))));

            return partitions.Partitions
                .Where(p => !_topicPartitions.ContainsKey(p))
                .Select(part => 
                {
                    var tp = new TopicPartition(_cluster, Topic, part, partitions.NextOffset(part));
                    _topicPartitions.Add(tp.PartitionId, tp);
                    return tp;
                });
        }


        public override string ToString()
        {
            return string.Format("Consumer: '{0}'", Topic);
        }

        private bool _isDisposed;

        public void Dispose()
        {
            if (_isDisposed)
                return;

            try
            {
                if (_partitionsSubscription != null && !_partitionsSubscription.IsDisposed)
                    _partitionsSubscription.Dispose();
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch {}

            try
            {
                // close and release the connections in the Cluster.
                if (_cluster != null && _cluster.State != Cluster.ClusterState.Disconnected)
                    _cluster.CloseAsync(TimeSpan.FromSeconds(5)).
                        ContinueWith(t => _log.Error(t.Exception, "Error when closing Cluster"), TaskContinuationOptions.OnlyOnFaulted);
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch(Exception e)
            {
                _log.Error(e, "Error in Dispose");
            }

            _isDisposed = true;
        }
    }
}
