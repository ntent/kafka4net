using System.Reactive.Disposables;
using kafka4net.Metadata;
using kafka4net.Protocols;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using kafka4net.Tracing;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace kafka4net.Internal
{
    /// <summary>
    /// Background task which monitors for recovery of failed partitions.
    /// </summary>
    class PartitionRecoveryMonitor
    {
        static readonly ILogger _log = Logger.GetLogger();

        private readonly Protocol _protocol;
        private readonly CancellationToken _cancel;

        // keep list of all known brokers so we can make requests to them to check that they're alive.
        private readonly Dictionary<int,BrokerMeta> _brokers = new Dictionary<int, BrokerMeta>();
        private readonly List<Task> _recoveryTasks = new List<Task>();

        private readonly Subject<MetadataResponse> _newMetadataEvent = new Subject<MetadataResponse>();
        public IObservable<MetadataResponse> NewMetadataEvents { get { return _newMetadataEvent; } }

        public Task Completion { get { return Task.WhenAll(_recoveryTasks); } }
        private readonly CompositeDisposable _subscriptionsDisposable = new CompositeDisposable(2);

        // keep a list of partitions that are failed, and their broker
        private readonly Dictionary<Tuple<string,int>,ErrorCode> _failedList = new Dictionary<Tuple<string, int>, ErrorCode>();

        static int _idGenerator;
        readonly int _id;

        public PartitionRecoveryMonitor(Cluster cluster, Protocol protocol, CancellationToken cancel)
        {
            _protocol = protocol;
            _cancel = cancel;
            _id = Interlocked.Increment(ref _idGenerator);

            _cancel.Register(() => {
                _subscriptionsDisposable.Dispose();
                _log.Debug("Cancel requested, unsubscribed new broker and partition state listeners");
            });

            _log.Debug("Created PartitionRecoveryMonitor {0}", this);
            EtwTrace.Log.RecoveryMonitor_Create(_id);

            // listen for new brokers
            _subscriptionsDisposable.Add(cluster.NewBrokers.Subscribe(
                broker =>
                {
                    // check and add this broker
                    if (_brokers.ContainsKey(broker.NodeId))
                        return;

                    if(_cancel.IsCancellationRequested)
                    {
                        _log.Warn($"#{_id} Ignoring new broker event because cancel has been requested");
                        return;
                    }

                    _recoveryTasks.Add(RecoveryLoop(broker)
                        .ContinueWith(t =>
                        {
                            _log.Debug("Recovery loop task for broker {0} completed with status {1}", broker.ToString(), t.Status);
                            _subscriptionsDisposable.Dispose();
                        }));
                    _brokers.Add(broker.NodeId, broker);
                },
                ex => _log.Error("Error thrown in NewBrokers subscription!")
                ));

            //
            // listen for any topic status changes and keep our "failed" list updated
            //
            _subscriptionsDisposable.Add(cluster.PartitionStateChanges.Subscribe(
                state =>
                {
                    var key = new Tuple<string, int>(state.Topic, state.PartitionId);
                    // check if it is failed or recovered, and remove or add to our failed list.
                    if (state.ErrorCode.IsSuccess())
                    {
                        if (_failedList.ContainsKey(key))
                        {
                            _log.Debug("#{0} Partition {1}-{2} is recovered. Removing from failed list.", _id, state.Topic, state.PartitionId);
                            EtwTrace.Log.RecoveryMonitor_PartitionRecovered(_id, state.Topic, state.PartitionId);
                            _failedList.Remove(key);
                        }
                    }
                    else
                    {
                        if (!_failedList.ContainsKey(key))
                        {
                            _log.Debug("#{0} Partition {1}-{2} is in error state {3}. Adding to failed list.", _id, state.Topic, state.PartitionId, state.ErrorCode);
                            EtwTrace.Log.RecoveryMonitor_PartitionFailed(_id, state.Topic, state.PartitionId, (int)state.ErrorCode);
                            _failedList.Add(key, state.ErrorCode);
                        }
                        else
                        {
                            _log.Debug("#{0} Partition {1}-{2} is updated but still errored. Updating ErrorCode in failed list.", _id, state.Topic, state.PartitionId);
                            EtwTrace.Log.RecoveryMonitor_PartitionFailedAgain(_id, state.Topic, state.PartitionId, (int)state.ErrorCode);
                            _failedList[key] = state.ErrorCode;
                        }
                    }
                },
                ex => _log.Error(ex, "#{0} Error thrown in PartitionStateChanges subscription!", _id)));
        }

        private async Task RecoveryLoop(BrokerMeta broker)
        {
            _log.Debug("{0} Starting recovery loop on broker: {1}", this, broker);
            EtwTrace.Log.RecoveryMonitor_RecoveryLoopStarted(_id, broker.Host, broker.Port, broker.NodeId);
            while (!_cancel.IsCancellationRequested)
            {
                //_log.Debug("RecoveryLoop iterating {0}", this);

                //
                // Check either there is any job for given broker
                //
                if (_failedList.Count == 0)
                {
                    // TODO: await for the list to receive 1st item instead of looping
                    await Task.Delay(1000, _cancel);
                    continue;
                }

                //
                // Query metadata from given broker for any failed topics.
                //
                MetadataResponse response;
                try
                {
                    EtwTrace.Log.RecoveryMonitor_SendingPing(_id, broker.Host, broker.Port);
                    response = await _protocol.MetadataRequest(new TopicRequest { Topics = _failedList.Keys.Select(t => t.Item1).Distinct().ToArray() }, broker, noTransportErrors: true);
                    EtwTrace.Log.RecoveryMonitor_PingResponse(_id, broker.Host, broker.Port);
                }
                catch (Exception ex)
                {
                    _log.Debug("PartitionRecoveryMonitor error. Broker: {0}, error: {1}", broker, ex.Message);
                    EtwTrace.Log.RecoveryMonitor_PingFailed(_id, broker.Host, broker.Port, ex.Message);
                    response = null;
                }

                if (response == null)
                {
                    await Task.Delay(1000, _cancel);
                    continue;
                }

                //
                // Join failed partitions with successful responses to find out recovered ones
                //
                Tuple<string, int, int>[] maybeHealedPartitions = (
                    from responseTopic in response.Topics
                    from responsePart in responseTopic.Partitions
                    let key = new Tuple<string, int>(responseTopic.TopicName, responsePart.Id)
                    where 
                        responseTopic.ErrorCode.IsSuccess()
                        && responsePart.ErrorCode.IsSuccess()
                        && _failedList.ContainsKey(key)
                    select Tuple.Create(responseTopic.TopicName, responsePart.Id, responsePart.Leader)
                    ).ToArray();

                if (_log.IsDebugEnabled)
                {
                    if (maybeHealedPartitions.Length == 0)
                    {
                        _log.Debug("Out of {0} partitions returned from broker {2}, none of the {3} errored partitions are healed. Current partition states for errored partitions: [{1}]",
                            response.Topics.SelectMany(t => t.Partitions).Count(),
                            string.Join(",", response.Topics
                                .SelectMany(t => t.Partitions.Select(p => new { t.TopicName, TopicErrorCode = t.ErrorCode, PartitionId = p.Id, PartitionErrorCode = p.ErrorCode }))
                                .Where(p => _failedList.ContainsKey(new Tuple<string, int>(p.TopicName, p.PartitionId)))
                                .Select(p => string.Format("{0}:{1}:{2}:{3}", p.TopicName, p.TopicErrorCode, p.PartitionId, p.PartitionErrorCode))),
                            broker,
                            _failedList.Count
                            );
                    }
                    else
                    {
                        var str = new StringBuilder();
                        foreach (var leader in maybeHealedPartitions.GroupBy(p => p.Item3, (i, tuples) => new { Leader = i, Topics = tuples.GroupBy(t => t.Item1) }))
                        {
                            str.AppendFormat(" Leader: {0}\n", leader.Leader);
                            foreach (var topic1 in leader.Topics)
                            {
                                str.AppendFormat("  Topic: {0} ", topic1.Key);
                                str.AppendFormat("[{0}]\n", string.Join(",", topic1.Select(t => t.Item2)));
                            }
                        }
                        _log.Debug("Healed partitions found by broker {0} (will check broker availability):\n{1}", broker, str.ToString());
                    }
                }

                if(EtwTrace.Log.IsEnabled()) 
                {
                    if (maybeHealedPartitions.Length != 0)
                    {
                        EtwTrace.Log.RecoveryMonitor_PossiblyHealedPartitions(_id, maybeHealedPartitions.Length);
                    }
                    else
                    {
                        EtwTrace.Log.RecoveryMonitor_NoHealedPartitions(_id);
                    }
                }

                //
                // Make sure that brokers for healed partitions are accessible, because it is possible that
                // broker B1 said that partition belongs to B2 and B2 can not be reach.
                // It is checked only that said broker responds to metadata request without exceptions.
                //
                var aliveChecks = maybeHealedPartitions.
                    GroupBy(p => p.Item3).
                    Select(async brokerGrp =>
                    {
                        BrokerMeta newBroker;
                        _brokers.TryGetValue(brokerGrp.Key, out newBroker);
                        if (newBroker == null)
                        {
                            newBroker = response.Brokers.SingleOrDefault(b => b.NodeId == brokerGrp.Key);

                            // If Cluster started when one of the brokers was down, and later it comes alive,
                            // it will be missing from our list of brokers. See issue #14.
                            _log.Debug("received MetadataResponse for broker that is not yet in our list: {0}", newBroker);

                            if (newBroker == null)
                            {
                                _log.Error("Got metadata response with partition refering to a broker which is not part of the response: {0}", response.ToString());
                                return;
                            }

                            // Broadcast only newly discovered broker and strip everything else, because this is the only
                            // confirmed data.
                            var filteredMeta = new MetadataResponse
                            {
                                Brokers = new[] { newBroker },
                                Topics = new TopicMeta[] { }
                            };

                            _newMetadataEvent.OnNext(filteredMeta);
                        }

                        try
                        {
                            EtwTrace.Log.RecoveryMonitor_CheckingBrokerAccessibility(_id, newBroker.Host, newBroker.Port, newBroker.NodeId);
                            MetadataResponse response2 = await _protocol.MetadataRequest(new TopicRequest { Topics = brokerGrp.Select(g=>g.Item1).Distinct().ToArray() }, newBroker, noTransportErrors: true);
                            EtwTrace.Log.RecoveryMonitor_BrokerIsAccessible(_id, newBroker.Host, newBroker.Port, newBroker.NodeId);

                            // success!
                            // raise new metadata event 
                            _log.Info("Alive brokers detected: {0} which responded with: {1}", newBroker, response2);

                            // Join maybe healed partitions with partitions which belong to alive broker
                            var confirmedHealedTopics =
                                (from maybeHealedPartition in brokerGrp
                                 from healedTopic in response2.Topics
                                 where healedTopic.TopicName == maybeHealedPartition.Item1
                                 from healedPart in healedTopic.Partitions
                                 where healedPart.Id == maybeHealedPartition.Item2 && healedPart.Leader == brokerGrp.Key
                                 group healedPart by new { healedTopic.TopicName, healedTopic.ErrorCode } into healedTopicGrp
                                 select healedTopicGrp
                                 );

                            
                            // broadcast only trully healed partitions which belong to alive broker
                            var filteredResponse = new MetadataResponse
                            {
                                Brokers = response2.Brokers, // we may broadcast more than 1 broker, but it should be ok because discovery of new broker metadata does not cause any actions
                                Topics = confirmedHealedTopics.
                                    Where(t => t.Any()). // broadcast only topics which have healed partitions
                                    Select(t => new TopicMeta
                                    {
                                        ErrorCode = t.Key.ErrorCode,
                                        TopicName = t.Key.TopicName,
                                        Partitions = t.ToArray()
                                    }).ToArray()
                            };

                            _log.Debug("Broadcasting filtered response {0}", filteredResponse);
                            if(EtwTrace.Log.IsEnabled())
                                foreach(var topic in filteredResponse.Topics)
                                    EtwTrace.Log.RecoveryMonitor_HealedPartitions(_id, newBroker.Host, newBroker.Port, newBroker.NodeId, topic.TopicName, string.Join(",", topic.Partitions.Select(p => p.Id)));
                            _newMetadataEvent.OnNext(filteredResponse);

                        }
                        catch (Exception e)
                        {
                            _log.Warn("Metadata points to broker but it is not accessible. Error: {0}", e.Message);
                        }
                    });

                // Wait for all checks to complete, otherwise, if a broker does not respond and hold connection open until tcp timeout,
                // we will keep accumulating responses in memory faster than they time out. See https://github.com/ntent-ad/kafka4net/issues/30
                await Task.WhenAll(aliveChecks.ToArray());

                await Task.Delay(3000, _cancel);
            }

            _log.Debug("RecoveryLoop exiting. Setting completion");
            EtwTrace.Log.RecoveryMonitor_RecoveryLoopStop(_id);
        }

        public override string ToString()
        {
            return string.Format("RecoveryMonitor Id: {0}", _id);
        }
    }
}
