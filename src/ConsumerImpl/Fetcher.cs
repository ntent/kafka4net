using System.Reactive.Subjects;
using kafka4net.Internal;
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
    /// One fetcher can contain multiple TopicPartitions subscribed.
    /// </summary>
    internal class Fetcher : IDisposable
    {
        private static readonly ILogger _log = Logger.GetLogger();

        private static int _nextId;
        private readonly int _id = Interlocked.Increment(ref _nextId);

        private readonly Cluster _cluster;
        private readonly BrokerMeta _broker;
        private readonly Protocol _protocol;
        private readonly CancellationToken _cancel;
        private readonly ConsumerConfiguration _consumerConfig;

        // keep list of TopicPartitions that are subscribed
        private readonly HashSet<TopicPartition> _topicPartitions = new HashSet<TopicPartition>();

        // this is the observable sequence of fetch responses returned from the FetchLoop
        private IObservable<FetchResponse> _fetchResponses;

        public int BrokerId { get { return _broker.NodeId; } }
        public BrokerMeta Broker { get { return _broker; } }

        // "bool" is a fake param, it does not carry any meaning
        readonly Subject<bool> _partitionsUpdated = new Subject<bool>();


        public Fetcher(Cluster cluster, BrokerMeta broker, Protocol protocol, ConsumerConfiguration consumerConfig, CancellationToken cancel)
        {
            _cluster = cluster;
            _broker = broker;
            _protocol = protocol;
            _cancel = cancel;

            _consumerConfig = consumerConfig;

            _fetchResponses = FetchLoop().Publish().RefCount();
            BuildReceivedMessages();

            _cancel.Register(() => _partitionsUpdated.OnNext(true));

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

            if (_log.IsDebugEnabled)
            {
                disposable.Add(Disposable.Create(() => _log.Debug("Fetcher #{0}: topicPartition is unsubscribing", topicPartition)));
            }

            _log.Debug("Fetcher #{0} added {1}", _id, topicPartition);

            // TODO: Add FlowControlState handling
            return disposable;
        }

        /// <summary>
        /// Compose the FetchResponses into ReceivedMessages
        /// </summary>
        internal IObservable<ReceivedMessage> ReceivedMessages { get; private set; }
        private void BuildReceivedMessages() {
            ReceivedMessages = _fetchResponses.SelectMany(response => {
                _log.Debug("#{0} Received fetch response", _id);
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
            .Do(
                _ => { }, 
                err => _log.Warn("Error in ReceivedMessages stream from broker {0}. Message: {1}", _broker, err.Message), 
                () => _log.Debug("ReceivedMessages stream for broker {0} is complete.", _broker)
            )
            .Publish().RefCount();
        }

        internal void PartitionsUpdated() 
        {
            _partitionsUpdated.OnNext(true);
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
                        _log.Debug("#{0} No partitions subscribed to fetcher. Waiting for _partitionsUpdated signal", _id);
                        await _partitionsUpdated.FirstAsync();
                        
                        if(_cancel.IsCancellationRequested)
                        {
                            _log.Debug("#{0}Cancel detected. Quitting FetchLoop", _id);
                            break;
                        }
                        
                        _log.Debug("#{0} Received _partitionsUpdated. Have {1} partitions subscribed", _id, _topicPartitions.Count);
                        continue;
                    }

                    // issue fetch 
                    FetchResponse fetch;
                    try
                    {
                        if(_log.IsDebugEnabled) 
                            _log.Debug("#{0}: sending FetchRequest: {1}", _id, fetchRequest);
                        
                        fetch = await _protocol.Fetch(fetchRequest, _broker.Conn);

                        // if any TopicPartitions have an error, fail them with the Cluster.
                        fetch.Topics.SelectMany(t => t.Partitions.Select(p => new PartitionStateChangeEvent(t.Topic, p.Partition, p.ErrorCode)))
                            .Where(ps => ps.ErrorCode != ErrorCode.NoError)
                            .ForEach(ps => _cluster.NotifyPartitionStateChange(ps));
                        
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
                        _log.Error(e, "#{0} Fetcher failed", _id);
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
