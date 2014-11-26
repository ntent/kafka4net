using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using kafka4net.Internal;

namespace kafka4net.ConsumerImpl
{
    class TopicPartition : IObserver<ReceivedMessage>
    {
        private static readonly ILogger _log = Logger.GetLogger();
        private Consumer _subscribedConsumer;
        private readonly Cluster _cluster;
        private readonly string _topic;
        private readonly int _partitionId;
        private readonly PartitionFetchState _partitionFetchState;

        // subscription handles
        private IDisposable _fetcherChangesSubscription;
        private IDisposable _currentfetcherSubscription;

        public TopicPartition(Cluster cluster, string topic, int partitionId, long initialOffset)
        {
            _cluster = cluster;
            _topic = topic;
            _partitionId = partitionId;
            _partitionFetchState = new PartitionFetchState(
                            PartitionId,
                            ConsumerStartLocation.SpecifiedLocations,
                            initialOffset);
        }

        public string Topic { get { return _topic; } }
        public int PartitionId { get { return _partitionId; } }

        public long CurrentOffset
        {
            get
            {
                // if we have not been initialized, something went wrong.
                if (_partitionFetchState == null)
                 throw new Exception("Partition Fetch State is not initialized. Has a consumer subscribed?");

                return _partitionFetchState.Offset;
            }
        }

        public IObservable<bool> FlowControl { get { return _subscribedConsumer.FlowControl; } }
        public bool FlowControlEnabled { get { return _subscribedConsumer.FlowControlEnabled; } }

        /// <summary>
        /// Subscribe a consumer to this topic partition. The act of subscribing 
        /// will cause this partition to seek out and connect to the "correct" Fetcher.
        /// </summary>
        /// <param name="consumer">The consumer subscribing. This is not IObservable because we want to subscribe to the FlowControlState of the consumer.</param>
        /// <returns></returns>
        public IDisposable Subscribe(Consumer consumer)
        {
            if (_subscribedConsumer != null && consumer != _subscribedConsumer)
                throw new Exception(string.Format("TopicPartition {0} is already subscribed to by a consumer!", this));

            _subscribedConsumer = consumer;

            // subscribe to fetcher changes for this partition.
            // We will immediately get a call with the "current" fetcher if it is available, and connect to it then.
            var fetchers = new List<Fetcher>();
            _fetcherChangesSubscription = _cluster
                .GetFetcherChanges(_topic, _partitionId, consumer.Configuration).
                Do(fetchers.Add).
                Subscribe(OnNewFetcher,OnFetcherChangesError,OnFetcherChangesComplete);
             
            _log.Debug("Starting {0} fetchers", fetchers.Count);
            fetchers.ForEach(fetcher => fetcher.PartitionsUpdated());

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
            if (newFetcher != null)
            {
                _log.Debug("{0} Received new fetcher. Fetcher: {1}. Subscribing to this fetcher.", this, newFetcher);
                _currentfetcherSubscription = newFetcher.Subscribe(this);
                newFetcher.PartitionsUpdated();
            }
        }

        private void OnFetcherChangesComplete()
        {
            // we aren't getting any more fetcher changes... shouldn't happen!
            _log.Warn("{0} Received FetcherChanges OnComplete event.... shouldn't happen unless we're shutting down.", this);
            DisposeImpl();
        }

        private void OnFetcherChangesError(Exception ex)
        {
            // we aren't getting any more fetcher changes... shouldn't happen!
            _log.Fatal(ex, "{0} Received FetcherChanges OnError event.... shouldn't happen!", this);
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
                _log.Error("{0} Recieved Message with no subscribed consumer. Discarding message.", this);
            }
            else
            {
                if (_partitionFetchState.Offset <= value.Offset)
                {
                    // if we are sending back an offset greater than we asked for then we likely skipped an offset. 
                    // Is this OK??
                    if (_partitionFetchState.Offset < value.Offset)
                        _log.Warn("{0} was expecting offset {1} but received larger offset {2}",this,_partitionFetchState.Offset,value.Offset);

                    _subscribedConsumer.OnMessageArrivedInput.OnNext(value);
                    // track that we handled this offset so the next time around, we fetch the next message
                    _partitionFetchState.Offset = value.Offset + 1;
                }
                else
                {
                    // the returned message offset was less than the offset we asked for, just skip this message.
                    _log.Debug("{0} Skipping message offset {1} as it is less than requested offset {2}",this,value.Offset,_partitionFetchState.Offset);
                }

            }
        }

        public void OnError(Exception error)
        {
            // don't pass this error up to the consumer, log it and wait for a new fetcher
            _log.Warn("{0} Recieved Error from Fetcher. Waiting for new or updated Fetcher. Message: {1}", this, error.Message);
            _currentfetcherSubscription.Dispose();
            _currentfetcherSubscription = null;
            _cluster.NotifyPartitionStateChange(new PartitionStateChangeEvent(Topic, PartitionId, ErrorCode.FetcherException));
        }

        public void OnCompleted()
        {
            // this shouldn't happen, but don't pass this up to the consumer, log it and wait for a new fetcher
            _log.Warn("{0} Recieved OnComplete from Fetcher. Fetcher may have errored out. Waiting for new or updated Fetcher.", this);
            _currentfetcherSubscription.Dispose();
            _currentfetcherSubscription = null;
        }

        private void DisposeImpl()
        {
            // just end the subscription to the current fetcher and to the consumer.
            if (_subscribedConsumer != null)
            {
                _subscribedConsumer.OnMessageArrivedInput.OnCompleted();
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

        public override string ToString()
        {
            return string.Format("'{0}' part: {1} offset: {2}",Topic, PartitionId, CurrentOffset);
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            var objTopicPartition = obj as TopicPartition;
            if (objTopicPartition == null)
                return false;

            return _cluster == objTopicPartition._cluster && _topic == objTopicPartition._topic && _partitionId == objTopicPartition._partitionId;
        }

        public override int GetHashCode()
        {
            unchecked // disable overflow, for the unlikely possibility that you
            {         // are compiling with overflow-checking enabled
                int hash = 27;
                hash = (13 * hash) + _topic.GetHashCode();
                hash = (13 * hash) + _partitionId.GetHashCode();
                return hash;
            }
        }
    }
}
