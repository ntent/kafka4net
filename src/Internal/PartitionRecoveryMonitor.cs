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

        public Task Completion { get { return _completion.Task; } }
        private readonly TaskCompletionSource<bool> _completion = new TaskCompletionSource<bool>();

        // keep a list of partitions that are failed, and their broker
        private readonly Dictionary<Tuple<string,int>,ErrorCode> _failedList = new Dictionary<Tuple<string, int>, ErrorCode>(); 

        public PartitionRecoveryMonitor(Router router, Protocol protocol, CancellationToken cancel)
        {
            _protocol = protocol;
            _cancel = cancel;

            // listen for new brokers
            router.NewBrokers.Subscribe(
                broker =>
                {
                    // check and add this broker
                    if (_brokers.ContainsKey(broker.NodeId)) return;
                    _recoveryTasks.Add(RecoveryLoop(broker));
                    _brokers.Add(broker.NodeId, broker);
                },
                ex => _log.Error("Error thrown in NewBrokers subscription!")
                );

            // listen for any topic status changes and keep our "failed" list updated
            router.PartitionStateChanges.Subscribe(
                state =>
                {
                    var key = new Tuple<string, int>(state.Item1, state.Item2);
                    // check if it is failed or recovered, and remove or add to our failed list.
                    if (state.Item3 == ErrorCode.NoError)
                    {
                         if (_failedList.ContainsKey(key))
                            _failedList.Remove(key);
                    }
                    else
                    {
                        if (!_failedList.ContainsKey(key))
                            _failedList.Add(key, state.Item3);
                        else
                            _failedList[key] = state.Item3;
                    }
                },
                ex => _log.Error(ex, "Error thrown in PartitionStateChanges subscription!"));
        }

        private async Task RecoveryLoop(BrokerMeta broker)
        {
            _log.Debug("Starting recovery loop on broker: {0}", broker);
            while (!_cancel.IsCancellationRequested)
            {

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
                    var str = new StringBuilder();
                    foreach (var leader in healedPartitions.GroupBy(p => p.Item3, p => p, (i, tuples) => new { Leader = i, Topics = tuples.GroupBy(t => t.Item1) }))
                    {
                        str.AppendFormat(" Leader: {0}\n", leader.Leader);
                        foreach (var topic1 in leader.Topics)
                        {
                            str.AppendFormat("  Topic: {0} ", topic1.Key);
                            str.AppendFormat("[{0}]\n", string.Join(",", topic1.Select(t => t.Item2)));
                        }
                    }
                    _log.Debug("Healed partitions found (will check broker availability):\n{0}", str.ToString());
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
                            _newMetadataEvent.OnNext(response2);

                            _log.Info("Alive brokers detected: {0}", newBroker);
                        }
                        catch (Exception e)
                        {
                            _log.Warn(e, "Metadata points to broker but it is not accessible");
                        }
                    });

                await Task.Delay(1000, _cancel);
            }
        }
    }
}
