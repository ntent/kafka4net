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
using kafka4net.Tracing;
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
        private readonly IObservable<bool> _flowControl;

        // keep list of TopicPartitions that are subscribed
        private readonly HashSet<TopicPartition> _topicPartitions = new HashSet<TopicPartition>();

        // this is the observable sequence of fetch responses returned from the FetchLoop
        private IObservable<FetchResponse> _fetchResponses;

        public int BrokerId { get { return _broker.NodeId; } }
        public BrokerMeta Broker { get { return _broker; } }

        // "bool" is a fake param, it does not carry any meaning
        readonly Subject<bool> _wakeupSignal = new Subject<bool>();


        public Fetcher(Cluster cluster, BrokerMeta broker, Protocol protocol, ConsumerConfiguration consumerConfig, CancellationToken cancel)
        {
            _cluster = cluster;
            _broker = broker;
            _protocol = protocol;
            _cancel = cancel;

            _consumerConfig = consumerConfig;
            
            _fetchResponses = FetchLoop().Publish().RefCount();
            BuildReceivedMessages();

            _cancel.Register(() => _wakeupSignal.OnNext(true));

            if(_log.IsDebugEnabled)
                _log.Debug("Created new fetcher #{0} for broker: {1}", _id, _broker);
            EtwTrace.Log.FetcherStart(_id, consumerConfig.Topic);
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
        /// Keeps track of the subscribed partitions in order to not fetch messages if the FlowControlState is Closed.
        /// </summary>
        /// <param name="topicPartition"></param>
        /// <returns></returns>
        public IDisposable Subscribe(TopicPartition topicPartition)
        {
            _topicPartitions.Add(topicPartition);
            EtwTrace.Log.FetcherPartitionSubscribed(_id, topicPartition.PartitionId);

            // cleanup
            var topicPartitionCleanup = Disposable.Create(() => _topicPartitions.Remove(topicPartition));
            var receivedMessagesSubscriptionCleanup = ReceivedMessages.Where(rm => rm.Topic == topicPartition.Topic && rm.Partition == topicPartition.PartitionId)
                    .Subscribe(topicPartition);
            var flowControlCleanup = topicPartition.FlowControl.
                // we need to wake up from waiting loop any time flow control hits low watermark and becomes enabled
                Where(enabled => enabled).
                Subscribe(_ => _wakeupSignal.OnNext(true));
            
            var cleanup = new CompositeDisposable
            {
                topicPartitionCleanup,
                receivedMessagesSubscriptionCleanup,
                flowControlCleanup
            };

            if (_log.IsDebugEnabled)
            {
                cleanup.Add(Disposable.Create(() => _log.Debug("Fetcher #{0} {1} topicPartition is unsubscribing", _id, topicPartition)));
            }

            _log.Debug("Fetcher #{0} added {1}", _id, topicPartition);

            return cleanup;
        }

        /// <summary>
        /// Compose the FetchResponses into ReceivedMessages
        /// </summary>
        internal IObservable<ReceivedMessage> ReceivedMessages { get; private set; }
        private void BuildReceivedMessages() {
            ReceivedMessages = _fetchResponses.SelectMany(response => {
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
            }).
            Do(msg => EtwTrace.Log.FetcherMessage(_id, msg.Key != null ? msg.Key.Length : -1, msg.Value != null ? msg.Value.Length : -1, msg.Offset, msg.Partition, msg.Topic))
            .Do(
                _ => { }, 
                err => _log.Warn("Error in ReceivedMessages stream from broker {0}. Message: {1}", _broker, err.Message), 
                () => _log.Debug("ReceivedMessages stream for broker {0} is complete.", _broker)
            )
            .Publish().RefCount();
        }

        internal void PartitionsUpdated() 
        {
            _wakeupSignal.OnNext(true);
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
                        Topics = _topicPartitions.
                            Where(tp => {
                                var enabled = tp.FlowControlEnabled;
                                if(!enabled)
                                    _log.Debug("#{0} Ignoring partition because flow controll is off {1}", _id, tp);
                                return enabled;
                            }).
                            GroupBy(tp=>tp.Topic).
                            Select(t => new FetchRequest.TopicData { 
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
                        _log.Debug("#{0} No partitions subscribed to fetcher. Waiting for _wakeupSignal signal", _id);
                        EtwTrace.Log.FetcherSleep(_id);
                        await _wakeupSignal.FirstAsync();
                        EtwTrace.Log.FetcherWakeup(_id);
                        
                        if(_cancel.IsCancellationRequested)
                        {
                            _log.Debug("#{0}Cancel detected. Quitting FetchLoop", _id);
                            break;
                        }
                        
                        _log.Debug("#{0} Received _wakeupSignal. Have {1} partitions subscribed", _id, _topicPartitions.Count);
                        continue;
                    }

                    // issue fetch 
                    FetchResponse fetch;
                    try
                    {
                        EtwTrace.Log.FetcherFetchRequest(_id, fetchRequest.Topics.Length, fetchRequest.Topics.Sum(td => td.Partitions.Length), _broker.Host, _broker.Port, _broker.NodeId);
                        fetch = await _protocol.Fetch(fetchRequest, _broker.Conn);
                        EtwTrace.Log.FetcherFetchResponse(_id);

                        // if any TopicPartitions have an error, fail them with the Cluster.
                        fetch.Topics.SelectMany(t => t.Partitions.Select(p => new PartitionStateChangeEvent(t.Topic, p.Partition, p.ErrorCode)))
                            .Where(ps => ps.ErrorCode != ErrorCode.NoError)
                            .ForEach(ps => _cluster.NotifyPartitionStateChange(ps));
                        
                        if (_log.IsDebugEnabled && fetch.Topics.Any(t=>t.Partitions.Any(p=>p.Messages.Length > 0)))
                            _log.Debug("#{0}: got FetchResponse from {2} with messages: {1}", _id, _broker.Conn, fetch.ToString(true));
                    }
                    //catch (TaskCanceledException)
                    //{
                    //    // Usually reason of fetch to time out is broker closing Tcp socket.
                    //    // Due to Tcp specifics, there are situations when closed connection can not be detected, 
                    //    // thus we need to implement timeout to detect it and restart connection.
                    //    _log.Info("#{0} Fetch timed out {1}", _id, this);

                    //    // Continue so that socket exception happen and handle exception
                    //    // in uniform way
                    //    continue;
                    //}
                    catch (ObjectDisposedException e)
                    {
                        if (!_cancel.IsCancellationRequested)
                        {
                            _log.Debug("#{0} connection closed", _id);
                            observer.OnError(e);
                            return;
                        }
                        
                        break;
                    }
                    catch (CorrelationLoopException e)
                    {
                        if (!_cancel.IsCancellationRequested)
                        {
                            _log.Debug("#{0} connection closed", _id);
                            observer.OnError(e);
                            return;
                        }
                        break;
                    }
                    catch (SocketException e)
                    {
                        if (!_cancel.IsCancellationRequested)
                        {
                            _log.Info(e, "#{0} Connection failed. {1}", _id, e.Message);
                            observer.OnError(e);
                            return;
                        }
                        break;
                    }
                    catch (Exception e)
                    {
                        if (!_cancel.IsCancellationRequested)
                        {
                            _log.Error(e, "#{0} Fetcher failed", _id);
                            observer.OnError(e);
                            return;
                        }
                        break;
                    }

                    // if timeout, we got empty response
                    if (fetch.Topics.Any(t => t.Partitions.Any(p => p.Messages.Length > 0))) 
                    { 
                        observer.OnNext(fetch);
                    }
                }

                _log.Info("Cancellation Requested. Shutting Down.");
                observer.OnCompleted();
            });
        }
    }
}
