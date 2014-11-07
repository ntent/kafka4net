using System.Linq;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Threading.Tasks;
using System.Runtime.Remoting.Messaging;
using kafka4net.ConsumerImpl;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
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

            OnMessageArrived = Observable.Create<ReceivedMessage>(async observer =>
            {
                // subscribe to all partitions
                var parts = await BuildTopicPartitionsAsync();
                parts.ForEach(part =>
                {
                    var partSubscription = part.Subscribe(this);
                    _partitionsSubscription.Add(partSubscription);
                });
                _log.Debug("Subscribed to partitions");

                // Relay messages from partition to consumer's output
                OnMessageArrivedInput.Subscribe(observer);

                // upon unsubscribe
                return () =>
                {
                    _partitionsSubscription.Dispose();
                    _topicPartitions.Clear();
                    // Do not wait for broker closure. Seems impossible in synchronous unsubscribe.
                    _cluster.CloseAsync(TimeSpan.FromSeconds(1));
                };
            }).
                SubscribeOn(_cluster.Scheduler).
            // allow multiple consumers to share the same subscription
            Publish().
            RefCount();
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
            });
        }

        /// <summary>
        ///  Provides access to the Cluster for this consumer
        /// </summary>
        public Cluster Cluster { get { return _cluster; } }

        /// <summary>For every patition, resolve offsets and build TopicPartition object</summary>
        internal async Task<IEnumerable<TopicPartition>> BuildTopicPartitionsAsync()
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
            var partitions = await _cluster.FetchPartitionOffsetsAsync(Topic);

            if (_log.IsDebugEnabled)
                _log.Debug("Consumer for topic {0} got time->offset resolved for location {1}. parts: [{2}]", Topic, Configuration.StartLocation, string.Join(",", partitions.OrderBy(p=>p.Partition).Select(p => string.Format("{0}:{1}", p.Partition, Configuration.StartLocation == ConsumerStartLocation.TopicHead ? p.Head : p.Tail))));

            return partitions
                .Where(pm => !_topicPartitions.ContainsKey(pm.Partition))
                .Select(part => 
                {
                    var tp = new TopicPartition(_cluster, Topic, part.Partition, Configuration.StartLocation == ConsumerStartLocation.TopicHead ? part.Head : part.Tail);
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

        /// <summary>
        /// TODO: Remove this!
        /// </summary>
        /// <param name="recoveredPartitions"></param>
        internal void OnPartitionsRecovered(IObservable<FetchResponse.TopicFetchData> recoveredPartitions)
        {
            throw new NotImplementedException();
            //recoveredPartitions.
            //    Subscribe(fetch => (
            //        from part in fetch.Partitions
            //        from msg in part.Messages
            //        select new ReceivedMessage
            //        {
            //            Topic = fetch.Topic,
            //            Partition = part.Partition,
            //            Key = msg.Key,
            //            Value = msg.Value
            //        }).
            //        ForEach(msg => _events.OnNext(msg))
            //    );
        }
    }
}
