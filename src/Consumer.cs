using System.Linq;
using System.Reactive.Threading.Tasks;
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
    public class Consumer : ISubject<ReceivedMessage>, IDisposable
    {
        private static readonly ILogger _log = Logger.GetLogger();

        internal ConsumerConfiguration Configuration { get; private set; }
        internal string Topic { get { return Configuration.Topic; } }

        private readonly Router _router;
        private readonly Dictionary<int, TopicPartition> _topicPartitions = new Dictionary<int, TopicPartition>(); 

        // with new organization, use a Subject to connect to all TopicPartitions
        private readonly Subject<ReceivedMessage> _receivedMessageStream = new Subject<ReceivedMessage>();
 
        // we should only ever subscribe once, and we want to keep that around to check if it was disposed when we are disposed
        private readonly SingleAssignmentDisposable _subscription = new SingleAssignmentDisposable();

        /// <summary>
        /// Create a new consumer using the specified configuration. See @ConsumerConfiguration
        /// </summary>
        /// <param name="consumerConfig"></param>
        public Consumer(ConsumerConfiguration consumerConfig)
        {
            Configuration = consumerConfig;
            _router = new Router(consumerConfig.SeedBrokers);
        }

        /// <summary>
        /// Pass connection method through from Router
        /// </summary>
        /// <returns></returns>
        public Task ConnectAsync()
        {
            return _router.ConnectAsync();
        }

        /// <summary>
        /// Ensures the subscription is disposed and closes the consumer's router.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public async Task CloseAsync(TimeSpan timeout)
        {
            await await _router.Scheduler.Ask(() =>
            {
                if (_subscription != null && ! _subscription.IsDisposed)
                    _subscription.Dispose();
                return _router.Close(timeout);
            });
        }

        /// <summary>
        /// Subscribes an observer to this consumer's stream of messages.
        /// </summary>
        /// <param name="observer">The observer of the messages.</param>
        /// <returns>An IDisposable representing the subscription handle. Dispose of the observable to close this subscription.</returns>
        public IDisposable Subscribe(IObserver<ReceivedMessage> observer)
        {
            return _router.Scheduler.Ask(() =>
            {
                if (_isDisposed)
                    throw new ObjectDisposedException("Consumer is already disposed.");

                if (_router.State != Router.BrokerState.Connected)
                    throw new Exception("Must connect the consumer by calling ConnectAsync before consuming.");

                // subscribe the observer to the ReceivedMessageStream
                var consumerSubscription = _receivedMessageStream.Subscribe(observer);

                // Get and subscribe to all TopicPartitions
                var subscriptions = new CompositeDisposable();

                // add the consumer subscription to the composite disposable so that everything is cancelled when disposed
                subscriptions.Add(consumerSubscription);

                // subscribe to all partitions
                GetTopicPartitions()
                    .Select(topicPartition => topicPartition.Subscribe(this))
                    .Subscribe(subscriptions.Add);

                _subscription.Disposable = subscriptions;
                return subscriptions;
            }).Result;
        }

        private IObservable<TopicPartition> GetTopicPartitions()
        {
            var partitions =
                BuildTopicPartitionsAsync().ToObservable()
                .SelectMany(p => p);
            return partitions;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        internal async Task<IObservable<TopicPartition>> BuildTopicPartitionsAsync()
        {
            // two code paths here. If we have specified locations to start from, just get the partition metadata, and build the TopicPartitions
            if (Configuration.StartLocation == ConsumerStartLocation.SpecifiedLocations)
            {
                var partitionMeta = await _router.GetTopicPartitionsAsync(Topic);
                return partitionMeta
                        .Where(pm=>!_topicPartitions.ContainsKey(pm.Id))
                        .Select(part =>
                            new TopicPartition(_router, Topic, part.Id, Configuration.PartitionOffsetProvider(part.Id)))
                        .ToObservable()
                        .Do(tp=>_topicPartitions.Add(tp.PartitionId,tp));
            }

            // no offsets provided. Need to issue an offset request to get start/end locations and use them for consuming
            var partitions = await _router.GetPartitionsInfo(Topic);

            if (_log.IsDebugEnabled)
                _log.Debug("Consumer for topic {0} got time->offset resolved for location {1}. parts: [{2}]", Topic, Configuration.StartLocation, string.Join(",", partitions.Select(p => string.Format("{0}-{1}", p.Partition, Configuration.StartLocation == ConsumerStartLocation.TopicHead ? p.Head : p.Tail))));

            return partitions
                .Where(pm => !_topicPartitions.ContainsKey(pm.Partition))
                .Select(part =>
                    new TopicPartition(_router, Topic, part.Partition, Configuration.StartLocation == ConsumerStartLocation.TopicHead ? part.Head : part.Tail))
                .ToObservable()
                .Do(tp => _topicPartitions.Add(tp.PartitionId, tp));
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
                if (_subscription != null && !_subscription.IsDisposed)
                    _subscription.Dispose();
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch {}

            try
            {
                // close and release the connections in the Router.
                if (_router != null && _router.State != Router.BrokerState.Disconnected)
                    _router.Close(TimeSpan.FromSeconds(5)).Wait();
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch { }

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

        public void OnNext(ReceivedMessage value)
        {
            // check that we have someone subscribed to the _receivedMessageSubject otherwise we could lose messages!
            if (!_receivedMessageStream.HasObservers)
                _log.Error("Got message from TopicPartition with no Observers!");

            // go ahead and pass it along anyway
            _receivedMessageStream.OnNext(value);
        }

        public void OnError(Exception error)
        {
            _log.Error("Exception sent from TopicPartition!",error);

            // go ahead and pass it along
            _receivedMessageStream.OnError(error);
        }

        /// <summary>
        /// This happens when an underlying TopicPartition calls OnCompleted... why would this occur?! Should not, other than when closing the whole consumer?
        /// </summary>
        public void OnCompleted()
        {
            _log.Info("Completed receiving from TopicPartition");

            // go ahead and pass it along
            _receivedMessageStream.OnCompleted();
        }
    }
}
