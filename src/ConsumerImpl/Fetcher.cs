using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Metadata;
using kafka4net.Protocol;
using kafka4net.Protocol.Requests;
using kafka4net.Protocol.Responses;
using kafka4net.Utils;

namespace kafka4net.ConsumerImpl
{
    /// <summary>
    /// Manages group of partitions to be fetched from single physical broker (connection).
    /// As different consumers can have same topic, but different wait time and maxBytes param,
    /// new fetcher will be created for each such a group.
    /// One fetcher can contain partitions from multiple topics as long as they share the same params.
    /// </summary>
    class Fetcher : IDisposable
    {
        /// <summary>BrokerId, MaxWaitTime, MinBytes</summary>
        public readonly Tuple<int, int, int> Key;

        private readonly BrokerMeta _broker;
        private readonly Transport _protocol;
        private readonly CancellationToken _cancel;
        private readonly Dictionary<Consumer,List<PartitionFetchState>> _consumerToPartitionsMap = new Dictionary<Consumer, List<PartitionFetchState>>();
        private Dictionary<string,List<PartitionFetchState>> _topicToPartitionsMap = new Dictionary<string, List<PartitionFetchState>>();
        static readonly ILogger _log = Logger.GetLogger();
        private IObservable<FetchResponse> _fetchResponses;
        readonly int _id = Interlocked.Increment(ref _nextId);
        static int _nextId;

        /// <summary>This constructor does not have partitions and must be followed with ResolveTime() call</summary>
        public Fetcher(BrokerMeta broker, Transport protocol, Consumer consumer, CancellationToken cancel)
        {
            _broker = broker;
            _protocol = protocol;
            _cancel = cancel;
            Key = Tuple.Create(broker.NodeId, consumer.MaxWaitTimeMs, consumer.MinBytes);

            if (_log.IsDebugEnabled)
                _log.Debug("Create new fetcher #{0} for consumer: {1}", _id, consumer);
        }

        public Fetcher(BrokerMeta broker, Transport protocol, Consumer consumer, List<PartitionFetchState> partitions, CancellationToken cancel)
        {
            _broker = broker;
            _protocol = protocol;
            _cancel = cancel;
            _consumerToPartitionsMap.Add(consumer, partitions);
            Key = Tuple.Create(broker.NodeId, consumer.MaxWaitTimeMs, consumer.MinBytes);
            _fetchResponses = FetchLoop();

            if(_log.IsDebugEnabled)
                _log.Debug("Created new fetcher #{0} for consumer: {1} with explicit partitions: {1}", _id, consumer, string.Join(",", partitions));
        }

        public override string ToString()
        {
            return string.Format("{0}:{1}/{2}", _broker.Host, _broker.Port, _broker.NodeId);
        }

        public void Dispose()
        {
            // TODO: On Dispose, clean up and exit the fetch loop, cancel any subscriptions.
            throw new NotImplementedException();
        }

        /// <summary>
        /// Handles the subscription of a new TopicPartition to this fetcher.
        /// Keeps track of the subscribed partitions in order to not fetch messages if the FlowControlState is Off.
        /// </summary>
        /// <param name="topicPartition"></param>
        /// <returns></returns>
        public IDisposable Subscribe(TopicPartition topicPartition)
        {
            // TODO: Add FlowControlState handling
            return ReceivedMessages.Where(rm => rm.Topic == topicPartition.Topic && rm.Partition == topicPartition.PartitionId)
                .Subscribe(topicPartition);

        }

        /// <summary>
        /// Compose the FetchResponses into ReceivedMessages
        /// </summary>
        private IObservable<ReceivedMessage> ReceivedMessages { get { 
            return _fetchResponses.SelectMany(response => {
                _log.Debug("#{0} Received fetch message", _id);
                
                return (
                    from topic in response.Topics
                    from part in topic.Partitions
                    from msg in part.Messages
                    select new ReceivedMessage
                    {
                        Topic = topic.Topic,
                        Partition = part.Partition,
                        Key = msg.Key,
                        Value = msg.Value,
                        Offset = msg.Offset
                    });
            });
        }}

        public IObservable<FetchResponse> AsObservable() { return _fetchResponses; }

        public Dictionary<string, List<PartitionFetchState>> GetOffsetStates() { return _topicToPartitionsMap;  }

        public bool HasTopic(string topic) { return _topicToPartitionsMap.ContainsKey(topic); }

        public bool HasConsumer(Consumer consumer) { return _consumerToPartitionsMap.ContainsKey(consumer); }

        public int BrokerId { get { return _broker.NodeId; } }

        public BrokerMeta Broker { get { return _broker; } }

        /// <summary>
        /// Initial start of partition fetching. ListeningGroup contains time and Fetch() needs to be called
        /// to convert Time into Offset.
        /// </summary>
        public async Task ResolveTime(Consumer consumer, int[] partitions)
        {
            // TODO: if fetcher is complete due to partition changing leader, while offset operation
            // in progress, what do we do?

            _log.Debug("Fetcher #{0} adding partitions to be time->offset resolved: parts: [{1}]", _id, string.Join(",", partitions));
            
            var req = new OffsetRequest
            {
                Time = consumer.Time,
                TopicName = consumer.Topic,
                Partitions = partitions
            };

            // issue request 
            // TODO: relaiability. If offset failed, try to recover
            // TODO: check offset return code
            var offset = await _protocol.GetOffsets(req, _broker.Conn);

            lock(_consumerToPartitionsMap)
            {
                List<PartitionFetchState> state;
                if(!_consumerToPartitionsMap.TryGetValue(consumer, out state)) {
                    _consumerToPartitionsMap.Add(consumer, state = new List<PartitionFetchState>());
                    _topicToPartitionsMap.Add(consumer.Topic, state);
                }
                
                // make sure there are no duplicates
                var same = partitions.Intersect(state.Select(p => p.PartId)).ToArray();
                if (same.Any())
                    _log.Error("Detected partitions which are already listening to. Topic: {0} partitions: {1}", consumer.Topic, string.Join(",", same.Select(p=>p.ToString()).ToArray()));

                var newStates = offset.Partitions.
                    Where(p => !same.Contains(p.Partition)).
                    // TODO: check offset.ErrorCode
                    Select(p => new PartitionFetchState(p.Partition, 0L, p.Offsets.First())).
                    ToArray();

                state.AddRange(newStates);

                _fetchResponses = FetchLoop();

                if (_log.IsDebugEnabled)
                    _log.Debug("Fetcher #{0} resolved time->offset. New fetch states: [{1}]", _id, string.Join(", ", newStates.AsEnumerable()));
            }
            
        }

        public void AdToListeningPartitions(Consumer consumer, List<PartitionFetchState> parts)
        {
            List<PartitionFetchState> partsOld;
            if (!_consumerToPartitionsMap.TryGetValue(consumer, out partsOld))
                _consumerToPartitionsMap.Add(consumer, parts);
            else
                partsOld.AddRange(parts);

            RebuildTopicMap();
        }

        void RebuildTopicMap()
        {
            _topicToPartitionsMap = _consumerToPartitionsMap.ToDictionary(kv => kv.Key.Topic, kv => kv.Value);
        }

        private IObservable<FetchResponse> FetchLoop()
        {
            return Observable.Create<FetchResponse>(async observer =>
            {
                while (!_cancel.IsCancellationRequested)
                {
                    var fetchRequest = new FetchRequest
                    {
                        MaxWaitTime = Key.Item2,
                        MinBytes = Key.Item3,
                        // TODO: sync _consumerToPartitionsMap
                        Topics = _consumerToPartitionsMap.Select(t => new FetchRequest.TopicData { 
                            Topic = t.Key.Topic,
                            Partitions = t.Value.
                                Select(p => new FetchRequest.PartitionData
                                {
                                    Partition = p.PartId,
                                    FetchOffset = p.Offset,
                                    MaxBytes = t.Key.MaxBytes
                                }).ToArray()
                        }).ToArray()
                    };
                    // issue fetch 
                    FetchResponse fetch;
                    try
                    {
                        if(_log.IsDebugEnabled) 
                            _log.Debug("#{0}: sending FetchRequest: {1}", _id, fetchRequest);
                        
                        fetch = await _protocol.Fetch(fetchRequest, _broker.Conn);
                        
                        if (_log.IsDebugEnabled)
                            _log.Debug("#{0}: got FetchResponse: {1}", _id, fetch);
                    }
                    catch (TaskCanceledException)
                    {
                        // Usually reason of fetch to time out is broker closing Tcp socket.
                        // Due to Tcp specifics, there are situations when closed connection can not be detected, 
                        // thus we need to implement timeout to detect it and restart connection.
                        _log.Info("Fetch #{0} timed out {1}", _id, this);

                        // Continue so that socket exception happen and handle exception
                        // in uniform way
                        continue;
                    }
                    catch (SocketException e)
                    {
                        _log.Info(e, "Connection failed. {0}", e.Message);
                        observer.OnError(e);
                        return;
                    }
                    catch (Exception e)
                    {
                        _log.Error(e, "Fetcher failed");
                        observer.OnError(e);
                        return;
                    }

                    // if timeout, we got empty response
                    if (fetch.Topics.Any(t => t.Partitions.Any(p => p.Messages.Length > 0))) 
                    { 
                        observer.OnNext(fetch);

                        // advance position to the message with the latest offset + 1
                        (
                            from fetchTopic in fetch.Topics
                            from fetchPartition in fetchTopic.Partitions
                            where fetchPartition.Messages.Length > 0    // when timed out, empty partition is returned
                            from partition in _topicToPartitionsMap[fetchTopic.Topic]
                            where partition.PartId == fetchPartition.Partition
                            select new { partition, fetchPartition.Messages.Last().Offset }
                        ).ForEach(p => p.partition.Offset = p.Offset + 1);
                    }
                }

                observer.OnCompleted();
            });
        }
    }
}
