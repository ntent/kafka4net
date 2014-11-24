using System.Reactive.Disposables;
using kafka4net.Metadata;
using kafka4net.Protocols;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using kafka4net.Utils;
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

            _log.Debug("Created PartitionRecoveryMonitor {0}", this);

            // listen for new brokers
            _subscriptionsDisposable.Add(cluster.NewBrokers.Subscribe(
                broker =>
                {
                    // check and add this broker
                    if (_brokers.ContainsKey(broker.NodeId)) return;
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
                    if (state.ErrorCode == ErrorCode.NoError)
                    {
                        if (_failedList.ContainsKey(key))
                        {
                            _log.Debug("#{0} Partition {1}-{2} is recovered. Removing from failed list.", _id, state.Topic, state.PartitionId);
                            _failedList.Remove(key);
                        }
                    }
                    else
                    {
                        if (!_failedList.ContainsKey(key))
                        {
                            _log.Debug("#{0} Partition {1}-{2} is in error state {3}. Adding to failed list.", _id, state.Topic, state.PartitionId, state.ErrorCode);
                            _failedList.Add(key, state.ErrorCode);
                        }
                        else
                        {
                            _log.Debug("#{0} Partition {1}-{2} is updated but still errored. Updating ErrorCode in failed list.", _id, state.Topic, state.PartitionId);
                            _failedList[key] = state.ErrorCode;
                        }
                    }
                },
                ex => _log.Error(ex, "#{0} Error thrown in PartitionStateChanges subscription!", _id)));
        }

        private async Task RecoveryLoop(BrokerMeta broker)
        {
            _log.Debug("{0} Starting recovery loop on broker: {1}", this, broker);
            while (!_cancel.IsCancellationRequested)
            {
                //_log.Debug("RecoveryLoop iterating {0}", this);

                //
                // Check either there is any job for given broker
                //
                if (_failedList.Count == 0)
                {
                    await Task.Delay(1000, _cancel);
                    continue;
                }

                //
                // Query metadata from given broker for any failed topics.
                //
                MetadataResponse response;
                try
                {
                    response = await _protocol.MetadataRequest(new TopicRequest { Topics = _failedList.Keys.Select(t => t.Item1).Distinct().ToArray() }, broker);
                }
                catch (Exception ex)
                {
                    _log.Debug("PartitionRecoveryMonitor error. Broker: {0}, error: {1}", broker, ex.Message);
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
                Tuple<string, int, int>[] healedPartitions = (
                    from responseTopic in response.Topics
                    from responsePart in responseTopic.Partitions
                    let key = new Tuple<string, int>(responseTopic.TopicName, responsePart.Id)
                    where 
                        responseTopic.TopicErrorCode == ErrorCode.NoError 
                        && responsePart.ErrorCode == ErrorCode.NoError 
                        && _failedList.ContainsKey(key)
                    select Tuple.Create(responseTopic.TopicName, responsePart.Id, responsePart.Leader)
                    ).ToArray();

                if (_log.IsDebugEnabled)
                {
                    if (healedPartitions.Length == 0)
                    {
                        _log.Debug("Out of {0} partitions returned from broker {2}, none of the {3} errored partitions are healed. Current partition states for errored partitions: [{1}]",
                            response.Topics.SelectMany(t => t.Partitions).Count(),
                            string.Join(",", response.Topics
                                .SelectMany(t => t.Partitions.Select(p => new { t.TopicName, t.TopicErrorCode, PartitionId = p.Id, PartitionErrorCode = p.ErrorCode }))
                                .Where(p => _failedList.ContainsKey(new Tuple<string, int>(p.TopicName, p.PartitionId)))
                                .Select(p => string.Format("{0}:{1}:{2}:{3}", p.TopicName, p.TopicErrorCode, p.PartitionId, p.PartitionErrorCode))),
                            broker,
                            _failedList.Count
                            );
                    }
                    else
                    {
                        var str = new StringBuilder();
                        foreach (var leader in healedPartitions.GroupBy(p => p.Item3, (i, tuples) => new { Leader = i, Topics = tuples.GroupBy(t => t.Item1) }))
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

                //
                // Make sure that brokers for healed partitions are accessible, because it is possible that
                // broker B1 said that partition belongs to B2 and B2 can not be reach.
                // It is checked only that said broker responds to metadata request without exceptions.
                //
                healedPartitions.
                    GroupBy(p => p.Item3).
                    ForEach(async brokerGrp =>
                    {
                        BrokerMeta newBroker;
                        _brokers.TryGetValue(brokerGrp.Key, out newBroker);
                        if (newBroker == null)
                        {
                            _log.Error("received MetadataResponse for broker that is not yet in our list!");
                            return;
                        }

                        try
                        {
                            MetadataResponse response2 = await _protocol.MetadataRequest(new TopicRequest { Topics = brokerGrp.Select(g=>g.Item1).Distinct().ToArray() }, newBroker);

                            // success!
                            // raise new metadata event 
                            _log.Info("Alive brokers detected: {0} which responded with: {1}", newBroker, response2);
                            
                            // broadcast only healed partitions which belong to newBroker
                            var filteredResponse = new MetadataResponse
                            {
                                Brokers = response2.Brokers,
                                Topics = response2.Topics.Select(t => new TopicMeta { 
                                    TopicErrorCode = t.TopicErrorCode,
                                    TopicName = t.TopicName,
                                    Partitions = t.Partitions.Where(p => healedPartitions.Any(hp => hp.Item1 == t.TopicName && hp.Item2 == p.Id)).ToArray()
                                }).ToArray()
                            };
                            
                            _log.Debug("Broadcasting filtered response {0}", filteredResponse);
                            _newMetadataEvent.OnNext(filteredResponse);

                        }
                        catch (Exception e)
                        {
                            _log.Warn("Metadata points to broker but it is not accessible. Error: {0}", e.Message);
                        }
                    });

                await Task.Delay(3000, _cancel);
            }

            _log.Debug("RecoveryLoop exiting. Setting completion");
        }

        public override string ToString()
        {
            return string.Format("RecoveryMonitor Id: {0}", _id);
        }
    }
}
