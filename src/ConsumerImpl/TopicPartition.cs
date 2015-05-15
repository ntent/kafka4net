using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using kafka4net.Internal;
using kafka4net.Protocols.Responses;

namespace kafka4net.ConsumerImpl
{
    /// <summary>
    /// Link Topics and Fetchers as many-to-many relation. One topic is served by many fetchers
    /// and single broker can fetch for multiple partitions of different topics.
    /// When metadata change, TopicPartition will change association with Fetcher, but association between topic and its TopicPartitions remains 
    /// the same.
    /// 
    /// TopicPartition-Fetcher subscription rules.
    /// TopicPartition can unsubscribe from Fetcher, when Consumer is closed.
    /// Fetcher can fail and TopicPartition needs to remove reference to it.
    /// Fetcher never completes, only fails.
    /// When Fetcher fails, it is TopicPartition's responsibility to broadcast partition status change.
    /// </summary>
    class TopicPartition
    {
        private static readonly ILogger _log = Logger.GetLogger();
        private Consumer _subscribedConsumer;
        private readonly Cluster _cluster;
        private readonly string _topic;
        private readonly int _partitionId;
        private readonly PartitionFetchState _partitionFetchState;
        
        private Fetcher _fetcher;

        private readonly SingleAssignmentDisposable _fetcherChangesSubscription = new SingleAssignmentDisposable();

        public TopicPartition(Cluster cluster, string topic, int partitionId, long initialOffset)
        {
            _cluster = cluster;
            _topic = topic;
            _partitionId = partitionId;
            _partitionFetchState = new PartitionFetchState(
                            PartitionId,
                            ConsumerLocation.SpecifiedLocations,
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

            _fetcherChangesSubscription.Disposable = SubscribeToFetcher();
             
            // give back a handle to close this topic partition.
            return Disposable.Create(DisposeImpl);
        }

        /// <summary>
        /// When receiving Partition avialable event, find fetcher responsible for it and subscribe.
        /// </summary>
        internal IDisposable SubscribeToFetcher()
        {
            return _cluster.PartitionStateChanges.
                Where(t => t.Topic == _topic && t.PartitionId == _partitionId).
                Where(state => state.ErrorCode.IsSuccess()).
                Subscribe(state =>
                {
                    // get or create the correct Fetcher for this topic/partition
                    var broker = _cluster.FindBrokerMetaForPartitionId(state.Topic, state.PartitionId);
                    if (_fetcher != null)
                        _fetcher.Unsubscribe(this);
                    _fetcher = _cluster.GetFetcher(broker, _subscribedConsumer.Configuration);
                    _fetcher.Subscribe(this);
                    _fetcher.State.Subscribe(_ => { }, OnFetcherError);

                    if (_log.IsDebugEnabled)
                        _log.Debug("Subscribed TopicPartition {0} to fetcher {1}", this, _fetcher);
                },
                ex => _log.Error(ex, "GetFetcherChages saw ERROR returning new fetcher."),
                () => _log.Error("SubscribeToFetcher saw COMPLETE from returning new fetcher."));
        }

        void OnFetcherError(Exception e)
        {
            _cluster.NotifyPartitionStateChange(new PartitionStateChangeEvent(Topic, PartitionId, ErrorCode.FetcherException));
            _fetcher = null;
            _log.Warn("{0} Recieved Error from Fetcher. Waiting for new or updated Fetcher. Error: {1}", this, e.Message);
        }

        /// <summary>
        /// Called when there is a new message received. Pass it to the Consumer
        /// </summary>
        public async Task OnFetchedMessages(ReceivedMessage[] batch)
        {
            if (_subscribedConsumer == null)
            {
                _log.Error("{0} Recieved Message with no subscribed consumer. Discarding message.", this);
                return;
            }

            // Sanity check
            if(!batch.All(msg => msg.Topic == _topic && msg.Partition == _partitionId))
                _log.Error("Wrong Topic/Partition");

            var newOffset = batch.Max(msg => msg.Offset);

            if (_partitionFetchState.Offset <= newOffset)
            {
                // if we are sending back an offset greater than we asked for then we likely skipped an offset. 
                // This happens when using Log Compaction (see https://kafka.apache.org/documentation.html#compaction )
                if (_partitionFetchState.Offset < newOffset)
                    _log.Info("{0} was expecting offset {1} but received larger offset {2}", this, _partitionFetchState.Offset, newOffset);

                await _subscribedConsumer.OnPartitionMessage(batch);
                // track that we handled this offset so the next time around, we fetch the next message
                _partitionFetchState.Offset = newOffset + 1;
            }
            else
            {
                // the returned message offset was less than the offset we asked for, just skip this message.
                _log.Debug("{0} Skipping message offset {1} as it is less than requested offset {2}",this, newOffset, _partitionFetchState.Offset);
            }
        }

        private void DisposeImpl()
        {
            // Stop listening to partitions available events and subscribing to fetchers
            _fetcherChangesSubscription.Dispose();

            if (_fetcher != null)
            {
                _fetcher.Unsubscribe(this);
                _fetcher = null;
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
