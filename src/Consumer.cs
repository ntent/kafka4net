using kafka4net.ConsumerImpl;
using kafka4net.Protocols.Responses;
using System;
using System.Collections.Generic;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;

namespace kafka4net
{
    public class Consumer : ISubject<ReceivedMessage>, IDisposable
    {
        private static readonly ILogger _log = Logger.GetLogger();

        internal ConsumerConfiguration Configuration { get; private set; }
        internal string Topic { get { return Configuration.Topic; } }

        private readonly Router _router;
        private readonly HashSet<TopicPartition> _topicPartitions = new HashSet<TopicPartition>(); 

        // with new organization, use a Subject to connect to all TopicPartitions
        private readonly Subject<ReceivedMessage> _receivedMessageStream = new Subject<ReceivedMessage>();
 
        // we should only every subscribe once, and we want to keep that around to check if it was disposed when we are disposed
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

        public Task CloseAsync(TimeSpan timeout)
        {
            return _router.Close(timeout);
        }

        public IDisposable Subscribe(IObserver<ReceivedMessage> observer)
        {
            if (_isDisposed)
                throw new ObjectDisposedException("Consumer is already disposed.");

            if (_router.State != Router.BrokerState.Connected)
                throw new Exception("Must connect the consumer by calling ConnectAsync before consuming.");

            // subscribe the observer to the ReceivedMessageStream
            var consumerSubscription = _receivedMessageStream.Subscribe(observer);

            // Get and subscribe to all TopicPartitions
            var subscriptions = new CompositeDisposable(GetTopicPartitions().Select(topicPartition => topicPartition.Subscribe(this)).ToEnumerable());

            
            // add the consumer subscription to the composite disposable so that everything is cancelled when disposed
            subscriptions.Add(consumerSubscription);

            _subscription.Disposable = subscriptions;
            return subscriptions;
        }

        private IObservable<TopicPartition> GetTopicPartitions()
        {
            var partitions = _router.PartitionStateChanges.Where(sc => sc.Item1 == Topic)
                .Select(part=> new TopicPartition(_router, Topic, part.Item2))
                .Where(tp => !_topicPartitions.Contains(tp))
                .Do(tp=>_topicPartitions.Add(tp));

            return partitions;
        }

        /// <summary>Resolve initial offsets of partitions and start fetching loop</summary>
        //internal async Task<IObservable<ReceivedMessage>> ResolveOffsetsAsync()
        //{
        //    var partitions = await _router.GetPartitionsInfo(Topic);

        //    // TODO: if fetcher is complete due to partition changing leader, while offset operation
        //    // in progress, what do we do?

        //    if (_log.IsDebugEnabled)
        //        _log.Debug("Fetcher #{0} adding partitions to be time->offset resolved: parts: [{1}]", _id, string.Join(",", partitions));

        //    IEnumerable<Tuple<int, long>> partOffsets;

        //    if (Configuration.StartLocation != ConsumerStartLocation.SpecifiedLocations)
        //    {
        //        // if implicit, find out offsets
        //        var req = new OffsetRequest
        //        {
        //            TopicName = Topic,
        //            Partitions = partitions.Select(id => new OffsetRequest.PartitionData
        //            {
        //                Id = id.,
        //                Time = (long)consumer.Configuration.StartLocation
        //            }).ToArray()
        //        };

        //        // issue request 
        //        // TODO: relaiability. If offset failed, try to recover
        //        // TODO: check offset return code
        //        var offset = await _protocol.GetOffsets(req, _broker.Conn);
        //        partOffsets = offset.Partitions.
        //            // p.Offsets.First(): if start from head, then Offsets will be [head], otherwise [tail,head],
        //            // thus First() will always get what we want.
        //            Select(p => Tuple.Create(p.Partition, p.Offsets.First()));
        //    }
        //    else
        //    {
        //        // if explicit offset provider exists
        //        partOffsets = partitions.
        //            Select(p => Tuple.Create(p, consumer.Configuration.PartitionOffsetProvider(p)));
        //    }

        //    lock (_consumerToPartitionsMap)
        //    {
        //        List<PartitionFetchState> state;
        //        if (!_consumerToPartitionsMap.TryGetValue(consumer, out state))
        //        {
        //            _consumerToPartitionsMap.Add(consumer, state = new List<PartitionFetchState>());
        //            _topicToPartitionsMap.Add(consumer.Topic, state);
        //        }

        //        // make sure there are no duplicates
        //        var same = partitions.Intersect(state.Select(p => p.PartId)).ToArray();
        //        if (same.Any())
        //            _log.Error("Detected partitions which are already listening to. Topic: {0} partitions: {1}", consumer.Topic, string.Join(",", same.Select(p => p.ToString()).ToArray()));

        //        var newStates = partOffsets.
        //            Where(p => !same.Contains(p.Item1)).
        //            // TODO: check offset.ErrorCode
        //            Select(p => new PartitionFetchState(p.Item1, 0L, p.Item2)).
        //            ToArray();

        //        state.AddRange(newStates);

        //        _fetchResponses = FetchLoop();

        //        if (_log.IsDebugEnabled)
        //            _log.Debug("Fetcher #{0} resolved time->offset. New fetch states: [{1}]", _id, string.Join(", ", newStates.AsEnumerable()));
        //    }
        //}


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
