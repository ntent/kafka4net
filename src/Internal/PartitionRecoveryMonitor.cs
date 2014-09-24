using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.ConsumerImpl;
using kafka4net.Metadata;
using kafka4net.Protocol;
using kafka4net.Protocol.Requests;
using kafka4net.Protocol.Responses;
using kafka4net.Utils;

namespace kafka4net.Internal
{
    /// <summary>
    /// Background task which monitors for recovery of failed partitions.
    /// </summary>
    class PartitionRecoveryMonitor
    {
        private readonly Transport _connection;
        private readonly CancellationToken _cancel;
        readonly Dictionary<string,List<PartitionFetchState>> _failedList = new Dictionary<string, List<PartitionFetchState>>();
        readonly Subject<Tuple<string,PartitionFetchState[]>> _events = new Subject<Tuple<string, PartitionFetchState[]>>();
        Subject<MetadataResponse> _newMetadataEvent = new Subject<MetadataResponse>();
        private IEnumerable<BrokerMeta> _brokers;

        public IObservable<Tuple<string, PartitionFetchState[]>> RecoveryEvents { get { return _events; } }
        public IObservable<MetadataResponse> NewMetadataEvents { get { return _newMetadataEvent; } }

        public Task Completion { get { return _completion.Task; } }
        TaskCompletionSource<bool> _completion = new TaskCompletionSource<bool>();

        static readonly ILogger _log = Logger.GetLogger();

        public PartitionRecoveryMonitor(IEnumerable<BrokerMeta> brokers, Transport _connection, CancellationToken cancel)
        {
            // TODO: if we change Broker to disover only needed topics upon connect, than brokers should be replaced with
            // IObservable<BrokerMeta>

            this._connection = _connection;
            _cancel = cancel;
            _cancel.Register(() => { 
                if(_failedList.Count == 0)
                    _completion.TrySetResult(true);
            });
            _brokers = brokers;

            RecoveryLoop(brokers);
        }

        public void StartRecovery(Dictionary<string, List<PartitionFetchState>> offsetStates)
        {
            if(_cancel.IsCancellationRequested)
                throw new BrokerException("Can not recover partition beause cancellation has been requested");

            lock(_failedList)
            {
                Merge(offsetStates);
            }
        }

        private void Merge(Dictionary<string, List<PartitionFetchState>> offsetStates)
        {
            foreach(var newTopic in offsetStates)
            {
                List<PartitionFetchState> offsets;
                if (!_failedList.TryGetValue(newTopic.Key, out offsets))
                {
                    _failedList.Add(newTopic.Key, new List<PartitionFetchState>(newTopic.Value));
                    _log.Debug("Started recovering new topic: '{0}', partitions: {1}", newTopic.Key, string.Join(",", newTopic.Value.Select(p=>p.ToString()).ToArray()));
                }
                else
                {
                    offsets.AddRange(newTopic.Value);
                    _log.Debug("Started recovering more partitions of topic: '{0}', partitions: {1}", newTopic.Key, string.Join(",", newTopic.Value.Select(p => p.ToString()).ToArray()));
                }
            }
        }

        void RecoveryLoop(IEnumerable<BrokerMeta> brokers)
        {
            // TODO: relay on Broker's discovering new brokers?
            // TODO: merge currently known brokers with seed brokers
            // TODO: refactor Brokers Monitor into a separate class

            brokers.Select(async broker =>
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
                    // Query metadata from given broker
                    //
                    var responseTask = _connection.MetadataRequest(new TopicRequest { Topics = _failedList.Keys.ToArray() }, broker);
                    await responseTask;
                    if (responseTask.IsFaulted)
                    {
                        _log.Debug("PartitionRecoveryMonitor error. Broker: {0}, error: {1}", broker, responseTask.Exception.Message);
                        await Task.Delay(1000, _cancel);
                        continue;
                    }
                    var response = responseTask.Result;

                    //
                    // Join failed partitions with successful responses to find out recovered ones
                    //
                    Tuple<string, PartitionFetchState, int>[] healedPartitions;   // topic, state, brokerId
                    lock (_failedList)
                    {
                        healedPartitions = (
                            from responseTopic in response.Topics
                            where responseTopic.TopicErrorCode == ErrorCode.NoError && _failedList.ContainsKey(responseTopic.TopicName)
                            from failedPart in _failedList[responseTopic.TopicName]
                            from responsePart in responseTopic.Partitions
                            where responsePart.Id == failedPart.PartId
                            select Tuple.Create(responseTopic.TopicName, failedPart, responsePart.Leader)
                        ).ToArray();
                    }
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
                    var alivePartitions = healedPartitions.
                        GroupBy(p => p.Item3).
                        Select(async brokerGrp =>
                        {
                            // TODO: should _brokers var be synced with Router because it is by-ref constructor parameter?
                            // TODO: any chance that newly discovered broker is not in _brokers yet?
                            var broker2 = _brokers.Single(b => b.NodeId == brokerGrp.Key);
                            MetadataResponse response2;
                            try
                            {
                                response2 = await _connection.MetadataRequest(new TopicRequest { Topics = _failedList.Keys.ToArray() }, broker2);
                            }
                            catch (Exception e)
                            {
                                throw new BrokerException(string.Format("Exception while connecting to broker {0}. Error: {1}", broker2, e.Message));
                            }

                            //
                            // success!
                            //

                            // raise new metadata event BEFORE recovered topic event 
                            _newMetadataEvent.OnNext(response2);

                            // remove partitons from failed list
                            lock (_failedList)
                            {
                                brokerGrp.GroupBy(b => b.Item1).    // group by topic
                                    Select(b => new { Topic = b.Key, States = b.Select(s => s.Item2).ToArray() }).
                                    // multiple brokers may respond with healed partitions, handle just one responce
                                    Where(topic => _failedList.ContainsKey(topic.Topic)).
                                    ForEach(topic =>
                                    {
                                        var failedParts = _failedList[topic.Topic];
                                        topic.States.ForEach(p => failedParts.Remove(p));
                                        if (failedParts.Count == 0)
                                            _failedList.Remove(topic.Topic);

                                        // raise partition recovered events
                                        _events.OnNext(Tuple.Create(topic.Topic, topic.States));
                                    });
                            }

                            return broker2;
                        }).ToArray();

                    if (alivePartitions.Length > 0)
                    {
                        alivePartitions.ToObservable().
                            Merge().
                            Subscribe(
                                b => _log.Info("Alive brokers detected: {0}", b),
                                e => _log.Debug(e, "Metadata points to broker but it is not accessible")
                            );
                    }

                    await Task.Delay(1000, _cancel);
                }
            }).ForEach(_ => {
                if (_failedList.Count == 0)
                    _completion.TrySetResult(true);
            });
        }
    }
}
