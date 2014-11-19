using System.Linq;
using System.Reactive.Concurrency;
using kafka4net.ConsumerImpl;
using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;
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

        /// <summary>
        /// Create a new consumer using the specified configuration. See @ConsumerConfiguration
        /// </summary>
        /// <param name="consumerConfig"></param>
        public Consumer(ConsumerConfiguration consumerConfig)
        {
            Configuration = consumerConfig;
            _cluster = new Cluster(consumerConfig.SeedBrokers);

            OnMessageArrived = Observable.Create<ReceivedMessage>(observer => 
            {
                // Subscription to OnMessageArrived is tricky because users would expect offsets to be resolved at the time
                // of OnMessageArrived.Subscribe(), which is synchronous. But fetchers offset resolution is async.
                // Using blocking Wait combined with ConfigureAwaiter(false) inside in SubscribeClient seems the only way to do it.
                var task = SubscribeClient(observer);
                task.Wait();
                return task.Result;
            }).
            Publish().
            RefCount()
            //.Do(msg=> _log.Debug("Received Message on final observable"))
            .ObserveOn(Scheduler.Default);
        }

        async Task<IDisposable> SubscribeClient(IObserver<ReceivedMessage> observer)
        {
            return await await _cluster.Scheduler.Ask(async () =>
            {
                // Relay messages from partition to consumer's output
                OnMessageArrivedInput
                    //.Do(msg=>_log.Debug("Received Message on ArrivedInput"))
                    .Subscribe(observer);

                // subscribe to all partitions
                var parts = await BuildTopicPartitionsAsync();
                parts.ForEach(part =>
                {
                    var partSubscription = part.Subscribe(this);
                    _partitionsSubscription.Add(partSubscription);
                });
                _log.Debug("Subscribed to partitions");

                // upon unsubscribe
                return Disposable.Create(() =>
                {
                    _partitionsSubscription.Dispose();
                    _topicPartitions.Clear();
                });
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Pass connection method through from Cluster
        /// </summary>
        /// <returns></returns>
        public Task ConnectAsync()
        {
            return _cluster.ConnectAsync();
        }

        /// <summary>
        /// Ensures the subscription is disposed and closes the consumer's Cluster.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public async Task CloseAsync(TimeSpan timeout)
        {
            await await _cluster.Scheduler.Ask(() =>
            {
                if (_partitionsSubscription != null && ! _partitionsSubscription.IsDisposed)
                    _partitionsSubscription.Dispose();
                return _cluster.CloseAsync(timeout);
            }).ConfigureAwait(false);
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
