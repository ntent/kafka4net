using System;
using System.Reactive.Disposables;

namespace kafka4net.ConsumerImpl
{
    class TopicPartition : IObserver<ReceivedMessage>
    {
        private static readonly ILogger _log = Logger.GetLogger();
        private Consumer _subscribedConsumer;
        private readonly Router _router;
        private readonly string _topic;
        private readonly int _partitionId;

        // subscription handles
        private IDisposable _fetcherChangesSubscription;
        private IDisposable _currentfetcherSubscription;

        public TopicPartition(Router router, string topic, int partitionId)
        {
            _router = router;
            _topic = topic;
            _partitionId = partitionId;
        }

        public string Topic { get { return _topic; } }
        public int PartitionId { get { return _partitionId; } }

        /// <summary>
        /// Subscribe a consumer to this topic partition. The act of subscribing 
        /// will cause this partition to seek out and connect to the "correct" Fetcher.
        /// </summary>
        /// <param name="consumer">The consumer subscribing. This is not IObservable because we want to subscribe to the FlowControlState of the consumer.</param>
        /// <returns></returns>
        public IDisposable Subscribe(Consumer consumer)
        {
            if (_subscribedConsumer != null && consumer != _subscribedConsumer)
                throw new Exception("TopicPartition is already subscribed to by a consumer!");

            _subscribedConsumer = consumer;

            // subscribe to fetcher changes for this partition.
            // We will immediately get a call with the "current" fetcher if it is available, and connect to it then.
            _fetcherChangesSubscription = _router.GetFetcherChanges(_topic, _partitionId)
                .Subscribe(OnNewFetcher,OnFetcherChangesError,OnFetcherChangesComplete);

            // give back a handle to close this topic partition.
            return Disposable.Create(DisposeImpl);

        }

        /// <summary>
        /// Handle the connection of a fetcher (potentially a new fetcher) for this partition
        /// </summary>
        /// <param name="newFetcher">The fetcher to use to fetch for this TopicPartition</param>
        private void OnNewFetcher(Fetcher newFetcher)
        {
            // first close the subscription to the old fetcher if there is one.
            if (_currentfetcherSubscription != null)
                _currentfetcherSubscription.Dispose();

            // now subscribe to the new fetcher. This will begin pumping messages through to the consumer.
            _currentfetcherSubscription = newFetcher.Subscribe(this);
        }

        private void OnFetcherChangesComplete()
        {
            // we aren't getting any more fetcher changes... shouldn't happen!
            _log.Error("Received FetcherChanges OnComplete event.... shouldn't happen!");
            DisposeImpl();
        }

        private void OnFetcherChangesError(Exception ex)
        {
            // we aren't getting any more fetcher changes... shouldn't happen!
            _log.Error(ex, "Received FetcherChanges OnError event.... shouldn't happen!");
            DisposeImpl();
        }

        /// <summary>
        /// Called when there is a new message received. Pass it to the Consumer
        /// </summary>
        /// <param name="value"></param>
        public void OnNext(ReceivedMessage value)
        {
            if (_subscribedConsumer == null)
            {
                _log.Error("Recieved Message with no subscribed consumer. Discarding message.");
            }
            else
            {
                _subscribedConsumer.OnNext(value);
            }
        }

        public void OnError(Exception error)
        {
            // don't pass this error up to the consumer, log it and wait for a new fetcher
            _log.Warn(error, "Recieved Error from Fetcher. Waiting for new or updated Fetcher.");
            _currentfetcherSubscription.Dispose();
            _currentfetcherSubscription = null;
        }

        public void OnCompleted()
        {
            // this shouldn't happen, but don't pass this up to the consumer, log it and wait for a new fetcher
            _log.Error("Recieved OnComplete from Fetcher. This shouldn't happen! Waiting for new or updated Fetcher.");
            _currentfetcherSubscription.Dispose();
            _currentfetcherSubscription = null;
        }

        private void DisposeImpl()
        {
            // just end the subscription to the current fetcher and to the consumer.
            if (_subscribedConsumer != null)
            {
                _subscribedConsumer.OnCompleted();
                _subscribedConsumer = null;
            }

            if (_fetcherChangesSubscription != null)
            {
                _fetcherChangesSubscription.Dispose();
                _fetcherChangesSubscription = null;
            }

            if (_currentfetcherSubscription != null)
            {
                _currentfetcherSubscription.Dispose();
                _currentfetcherSubscription = null;
            }
        }
    }
}
