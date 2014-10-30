using kafka4net.Metadata;
using kafka4net.Protocols;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.Utils;

namespace kafka4net.ConsumerImpl
{
    /// <summary>
    /// Manages group of partitions to be fetched from single physical broker (connection).
    /// As different consumers can have same topic, but different wait time and maxBytes param,
    /// new fetcher will be created for each such a group.
    /// One fetcher can contain partitions from multiple topics as long as they share the same params.
    /// </summary>
    internal class Fetcher : IDisposable
    {
        private static readonly ILogger _log = Logger.GetLogger();

        private static int _nextId;
        private readonly int _id = Interlocked.Increment(ref _nextId);

        private readonly Router _router;
        private readonly BrokerMeta _broker;
        private readonly Protocol _protocol;
        private readonly CancellationToken _cancel;
        private readonly ConsumerConfiguration _consumerConfig;

        // keep list of TopicPartitions that are subscribed
        private readonly HashSet<TopicPartition> _topicPartitions = new HashSet<TopicPartition>();

        // this is the observable sequence of fetch responses returned from the FetchLoop
        private readonly IObservable<FetchResponse> _fetchResponses;


        public Fetcher(Router router, BrokerMeta broker, Protocol protocol, ConsumerConfiguration consumerConfig, CancellationToken cancel)
        {
            _router = router;
            _broker = broker;
            _protocol = protocol;
            _cancel = cancel;

            _consumerConfig = consumerConfig;

            _fetchResponses = FetchLoop();

            if(_log.IsDebugEnabled)
                _log.Debug("Created new fetcher #{0} for broker: {1}", _id, _broker);
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
            _topicPartitions.Add(topicPartition);

            // create a disposable that allows us to remove the topic partition
            var disposable = new CompositeDisposable
            {
                Disposable.Create(() => _topicPartitions.Remove(topicPartition)),
                ReceivedMessages.Where(rm => rm.Topic == topicPartition.Topic && rm.Partition == topicPartition.PartitionId)
                    .Subscribe(topicPartition)
            };

            // TODO: Add FlowControlState handling
            return disposable;
        }

        /// <summary>
        /// Compose the FetchResponses into ReceivedMessages
        /// </summary>
        internal IObservable<ReceivedMessage> ReceivedMessages { get { 
            return _fetchResponses.SelectMany(response => {
                _log.Debug("#{0} Received fetch message", _id);
                
                return (
                    from topic in response.Topics
                    from part in topic.Partitions where part.ErrorCode == ErrorCode.NoError
                    from msg in part.Messages
                    select new ReceivedMessage
                    {
                        Topic = topic.Topic,
                        Partition = part.Partition,
                        Key = msg.Key,
                        Value = msg.Value,
                        Offset = msg.Offset
                    });
            })
            .Do(_ => { }, err => _log.Warn("Error received in ReceivedMessages stream from broker {0}. Message: {1}", _broker, err.Message), () => _log.Debug("ReceivedMessages stream for broker {0} is complete.", _broker));
        }}

        internal Tuple<string, int>[] AllListeningPartitions
        {
            get
            {
                return _topicPartitions.Select(tp=>new Tuple<string,int>(tp.Topic,tp.PartitionId)).ToArray();
            }
        }

        /*

        public IObservable<FetchResponse> AsObservable() { return _fetchResponses; }

        public Dictionary<string, List<PartitionFetchState>> GetOffsetStates() { return _topicToPartitionsMap;  }

        public bool HasTopic(string topic) { return _topicToPartitionsMap.ContainsKey(topic); }

        public bool HasConsumer(Consumer consumer) { return _consumerToPartitionsMap.ContainsKey(consumer); }

         */

        public int BrokerId { get { return _broker.NodeId; } }
        public BrokerMeta Broker { get { return _broker; } }

        /// <summary>
        /// TODO: Remove!
        /// </summary>
        /// <param name="consumer"></param>
        /// <param name="parts"></param>
        public void AddToListeningPartitions(Consumer consumer, List<PartitionFetchState> parts)
        {
            throw new NotImplementedException();
            //List<PartitionFetchState> partsOld;
            //if (!_consumerToPartitionsMap.TryGetValue(consumer, out partsOld))
            //    _consumerToPartitionsMap.Add(consumer, parts);
            //else
            //    partsOld.AddRange(parts);

            //RebuildTopicMap();
        }

        private IObservable<FetchResponse> FetchLoop()
        {
            return Observable.Create<FetchResponse>(async observer =>
            {
                while (!_cancel.IsCancellationRequested)
                {
                    var fetchRequest = new FetchRequest
                    {
                        MaxWaitTime = _consumerConfig.MaxWaitTimeMs,
                        MinBytes = _consumerConfig.MinBytesPerFetch,
                        Topics = _topicPartitions.GroupBy(tp=>tp.Topic).Select(t => new FetchRequest.TopicData { 
                            Topic = t.Key,
                            Partitions = t.
                                Select(p => new FetchRequest.PartitionData
                                {
                                    Partition = p.PartitionId,
                                    FetchOffset = p.CurrentOffset,
                                    MaxBytes = _consumerConfig.MaxBytesPerFetch
                                }).ToArray()
                        }).ToArray()
                    };

                    if (fetchRequest.Topics.Length == 0)
                    {
                        // no topics, yield for a little and let others run
                        await Task.Delay(_consumerConfig.MaxWaitTimeMs, _cancel);
                        continue;
                    }

                    // issue fetch 
                    FetchResponse fetch;
                    try
                    {
                        if(_log.IsDebugEnabled) 
                            _log.Debug("#{0}: sending FetchRequest: {1}", _id, fetchRequest);
                        
                        fetch = await _protocol.Fetch(fetchRequest, _broker.Conn);

                        // if any TopicPartitions have an error, fail them with the router.
                        fetch.Topics.SelectMany(t=> t.Partitions.Select(p => new Tuple<string, int, ErrorCode>(t.Topic, p.Partition, p.ErrorCode)))
                            .Where(ps => ps.Item3 != ErrorCode.NoError)
                            .ForEach(ps => _router.SendPartitionStateChange(ps));
                        
                        if (_log.IsDebugEnabled)
                            _log.Debug("#{0}: got FetchResponse: {1}", _id, fetch);
                    }
                    catch (TaskCanceledException)
                    {
                        // Usually reason of fetch to time out is broker closing Tcp socket.
                        // Due to Tcp specifics, there are situations when closed connection can not be detected, 
                        // thus we need to implement timeout to detect it and restart connection.
                        _log.Info("#{0} Fetch timed out {1}", _id, this);

                        // Continue so that socket exception happen and handle exception
                        // in uniform way
                        continue;
                    }
                    catch (SocketException e)
                    {
                        _log.Info(e, "#{0} Connection failed. {1}", _id, e.Message);
                        observer.OnError(e);
                        return;
                    }
                    catch (Exception e)
                    {
                        _log.Error(e, "#{0} Fetcher failed",_id);
                        observer.OnError(e);
                        return;
                    }

                    // if timeout, we got empty response
                    if (fetch.Topics.Any(t => t.Partitions.Any(p => p.Messages.Length > 0))) 
                    { 
                        observer.OnNext(fetch);
                    }
                }

                observer.OnCompleted();
            });
        }
    }
}
