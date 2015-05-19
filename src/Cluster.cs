using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
using kafka4net.ConsumerImpl;
using kafka4net.Internal;
using kafka4net.Metadata;
using kafka4net.Protocols;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using kafka4net.Tracing;
using kafka4net.Utils;

namespace kafka4net
{
    /// <summary>
    /// Manage connections, metadata and makes routing decisions.
    /// </summary>
    public class Cluster
    {
        private readonly string _seedBrokers;

        internal enum ClusterState
        {
            Disconnected,
            Connected,
            Closed // closed means we already closed down, cannot reconnect.
        }

        private static readonly ILogger _log = Logger.GetLogger();

        private readonly Protocol _protocol;

        private ClusterState _state = ClusterState.Disconnected;
        internal ClusterState State { get { return _state; } }

        private readonly MetadataResponse _metadata = new MetadataResponse { Brokers = new BrokerMeta[0], Topics = new TopicMeta[0]};
        
        //
        // indexed metadata
        private Dictionary<string,PartitionMeta[]> _topicPartitionMap = new Dictionary<string, PartitionMeta[]>();
        private Dictionary<PartitionMeta, BrokerMeta> _partitionBrokerMap = new Dictionary<PartitionMeta, BrokerMeta>();
        private readonly ConcurrentDictionary<BrokerMeta, Fetcher> _activeFetchers = new ConcurrentDictionary<BrokerMeta, Fetcher>();

        // Single Threaded Scheduler to handle all async methods in the library
        internal readonly WatchdogScheduler Scheduler;
        private Timer _watchdogTimer;
        internal volatile Thread CurrentWorkerThread;
        public Action<WorkingThreadHungException> OnThreadHang;

        // Cluster ID (unique number for each Cluster instance. used in debugging messages.)
        private static int _idCount;
        private readonly int _id = Interlocked.Increment(ref _idCount);

        // Monitor for recovering partitions when they enter an error state.
        PartitionRecoveryMonitor _partitionRecoveryMonitor;

        private readonly CancellationTokenSource _cancel = new CancellationTokenSource();

        private readonly CompositeDisposable _shutdownDisposable = new CompositeDisposable();

        private Cluster() 
        {
            Scheduler = new WatchdogScheduler(
                new EventLoopScheduler(ts =>
                {
                    var thread = new Thread(ts) { Name = "kafka-scheduler " + _id, IsBackground = true };
                    CurrentWorkerThread = thread;
                    _log.Debug("Created new thread '{0}'", thread.Name);
                    return thread;
                }));

            // Init synchronization context of scheduler thread
            Scheduler.Schedule(() => SynchronizationContext.SetSynchronizationContext(new RxSyncContextFromScheduler(Scheduler)));

            var hangTimeout = TimeSpan.FromMinutes(1);
            Scheduler.DelaySampler.Where(_ => _ > hangTimeout).
                FirstOrDefaultAsync().
                Subscribe(_ =>
                {
                    var thread = CurrentWorkerThread;
                    if (thread != null)
                    {
                        var msg = string.Format("Driver handler thread is stuck for {0} (>{1}ms). Aborting!", _, hangTimeout);
                        _log.Fatal(msg);
                        thread.Abort();
                        if (OnThreadHang != null)
                            OnThreadHang(new WorkingThreadHungException(msg));
                    }
                });


            // build a subject that relays any changes to a partition's error state to anyone observing.
            BuildPartitionStateChangeSubject();
            _newBrokerSubject = BuildnewBrokersSubject();
        }

        /// <summary>
        /// ConnectAsync and fetch list of brokers and metadata.
        /// </summary>
        /// <param name="seedBrokers">Comma separated list of seed brokers. Port numbers are optional.
        /// <example>192.168.56.10,192.168.56.20:8081,broker3.local.net:8181</example>
        /// </param>
        public Cluster(string seedBrokers) : this()
        {
            _seedBrokers = seedBrokers;
            _protocol = new Protocol(this);
            _state = ClusterState.Disconnected;
        }

        private void HandleTransportError(Exception e, BrokerMeta broker)
        {
            _log.Info("Handling TransportError for broker {0}", broker);
            (
                from topic in _metadata.Topics
                from part in topic.Partitions
                where part.Leader == broker.NodeId
                select new { topic.TopicName, part }
            ).ForEach(p =>
            {
                _log.Debug("Marking topic {2} partition {1} with transport error for broker {0}", broker, p.part, p.TopicName);
                p.part.ErrorCode = ErrorCode.TransportError;
                EtwTrace.Log.MetadataTransportError(p.TopicName, _id, p.part.Id, p.part.Leader);
                _partitionStateChangesSubject.OnNext(new PartitionStateChangeEvent(p.TopicName, p.part.Id, ErrorCode.TransportError));
            });
        }

        private void CheckConnected()
        {
            if (_state != ClusterState.Connected)
                throw new BrokerException("Cluster is not connected");
        }

        #region Public Accessible Methods

        /// <summary>
        /// Connect to the cluster. Connects to all seed addresses, and fetches initial metadata for the cluster.
        /// </summary>
        /// <returns></returns>
        public async Task ConnectAsync()
        {
            await await Scheduler.Ask(async () =>
            {
                // we cannot reconnect if we have closed already.
                if (_state == ClusterState.Closed)
                    throw new BrokerException("Cluster is already closed. Cannot reconnect. Please create a new Cluster.");

                if (_state != ClusterState.Disconnected)
                    return;

                _log.Debug("Connecting");

                var initBrokers = Connection.ParseAddress(_seedBrokers).
                    Select(seed => new BrokerMeta 
                    {
                        Host = seed.Item1,
                        Port = seed.Item2,
                        NodeId = -99
                    }).ToArray();
                EtwTrace.Log.ClusterStarting(_id);

                var initMeta = new MetadataResponse { Topics = new TopicMeta[0], Brokers = initBrokers };

                MergeTopicMeta(initMeta);
                _state = ClusterState.Connected;

                // start up a recovery monitor to watch for recovered partitions
                _partitionRecoveryMonitor = new PartitionRecoveryMonitor(this, _protocol, _cancel.Token);
                // Merge metadata that recovery monitor discovers
                _partitionRecoveryMonitor.NewMetadataEvents.Subscribe(MergeTopicMeta, ex => _log.Error(ex, "Error thrown by RecoveryMonitor.NewMetadataEvents!"));
                _log.Debug("Connected");
                EtwTrace.Log.ClusterStarted(_id);
            }).ConfigureAwait(false);
        }

        /// <summary>
        /// Close all resources related to the cluster. Attempts to do so in a graceful manner, flushing
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public async Task CloseAsync(TimeSpan timeout)
        {
            if (_state == ClusterState.Disconnected || _state == ClusterState.Closed)
                return;

            _log.Info("#{0} Closing...", _id);
            EtwTrace.Log.ClusterStopping(_id);

            // wait on the partition recovery monitor and all of the correlation loops to shut down.
            var success =
                await await Scheduler.Ask(async () =>
                    await CloseAsyncImpl().TimeoutAfter(timeout)
                ).ConfigureAwait(false);

            // release references in subscriptions
            _shutdownDisposable.Dispose();

            if (!success)
            {
                _log.Error("Timed out");
                EtwTrace.Log.ClusterError(_id, "Timeout on close");
                if (!_partitionRecoveryMonitor.Completion.IsCompleted)
                    _log.Error("_partitionRecoveryMonitor timed out");
            }
            else
            {
                _log.Info("#{0} Closed", _id);
                EtwTrace.Log.ClusterStopped(_id);
            }

            _state = ClusterState.Disconnected;
        }

        private async Task CloseAsyncImpl()
        {
            _cancel.Cancel();

            _log.Debug("#{0} Awaiting PartitionRecoveryMonitor exit.", _id);
            try
            {
                await _partitionRecoveryMonitor.Completion;//.ConfigureAwait(false);
            }
            catch (OperationCanceledException) {}

            _log.Info("#{0} PartitionRecoveryMonitor closed. Waiting on closing of connections.", this);
            try
            {
                await (Task.WhenAll(_metadata.Brokers.Select(b => b.Conn.ShutdownAsync()).ToArray()));//.ConfigureAwait(false);
            }
            catch (TaskCanceledException) { }

            _log.Info("#{0} Finished Shutting down connections.", this);
        }

        /// <summary>Get list of all topics which are already cached. Does not issue a metadata request.</summary>
        public async Task<string[]> GetAllTopicsAsync()
        {
            CheckConnected();

            return await await Scheduler.Ask(async () =>
            {
                var meta = await _protocol.MetadataRequest(new TopicRequest { Topics = new string[0] });
                MergeTopicMeta(meta);
                return _metadata.Topics.Select(t => t.TopicName).ToArray();
            });
        }


        /// <summary>
        /// Get list of partitions and the offsets at the given location. If the partition is empty, this method returns 0 for all offsets regardless of location.
        /// Tail is pointing to the offest AFTER the last message, i.e. offset of the message to be written next.
        /// This function will force fetch requests, i.e. it does not use cached metadata.
        /// </summary>
        /// <param name="topic">topic name for which to retrieve offsets for</param>
        /// <param name="startLocation">The location you're interested in. Pass either TopicHead OR TopicTail, but NOT 'SpecifiedLocations'. </param>
        /// <returns></returns>
        public Task<TopicPartitionOffsets> FetchPartitionOffsetsAsync(string topic, ConsumerLocation startLocation)
        {
            if (startLocation == ConsumerLocation.SpecifiedLocations)
                throw new ArgumentException("startLocation must be either TopicHead or TopicTail");

            return FetchPartitionOffsetsImplAsync(topic, (long) startLocation);
        }

        /// <summary>
        /// Get list of partitions and the maximum offsets prior to the given time. This functionality 
        /// depends on the Kafka Broker settings that control log segment file rolling. These settings are log.roll.hours and log.segment.bytes
        /// If these are set too high, and new segment files are not created often, this method will tend to return very coarse results.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="startDateTime"></param>
        /// <returns></returns>
        public Task<TopicPartitionOffsets> FetchPartitionOffsetsAsync(string topic, DateTimeOffset startDateTime)
        {
            return FetchPartitionOffsetsImplAsync(topic, startDateTime.DateTimeToEpochMilliseconds());
        }
        

        /// <summary>
        /// Get list of partitions and their offsets at the given time. 
        /// </summary>
        /// <param name="topic">topic name for which to retrieve offsets for</param>
        /// <param name="time">max timestamp for message offsets. returns the largest offsets that are before the given time. Pass -2L for TopicHead, and -1L for TopicTail.</param>
        /// <returns></returns>
        private async Task<TopicPartitionOffsets> FetchPartitionOffsetsImplAsync(string topic, long time)
        {
            CheckConnected();

            var ret = await await Scheduler.Ask(async () => {
                var numAttempts = -1;
                while (!_cancel.IsCancellationRequested)
                {
                    numAttempts++;
                    bool retry;
                    Exception exception;
                    try
                    {
                        // get partition list
                        var parts = (await GetOrFetchMetaForTopicAsync(topic)).
                            Select(p => new OffsetRequest.PartitionData {
                                    Id = p.Id,
                                    Time = time,
                                    MaxNumOffsets = 1
                                }).ToArray();

                        // group parts by broker
                        var requests = (
                            from part in parts
                            let broker = FindBrokerMetaForPartitionId(topic, part.Id)
                            group part by broker into brokerGrp
                            let req = new OffsetRequest { TopicName = topic, Partitions = brokerGrp.ToArray() }
                            select _protocol.GetOffsets(req, brokerGrp.Key.Conn)
                            ).ToArray();

                        await Task.WhenAll(requests);

                        // handle recoverable errors, such as tcp transport exceptions (with limited retry)
                        // or partition relocation error codes
                        if(requests.Any(r => r.IsFaulted))
                            throw new AggregateException("Failure when getting offsets info", requests.Where(r => r.IsFaulted).Select(r => r.Exception));

                        var partitions = (from r in requests
                                         from part in r.Result.Partitions
                                         select part).ToArray();

                        if (partitions.Any(p=> !p.ErrorCode.IsSuccess()))
                            throw new Exception(string.Format("Partition Errors: [{0}]", string.Join(",", partitions.Select(p=>p.Partition + ":" + p.ErrorCode))));

                        //if (partitions.Any(p => (p.Offsets.Length == 1 ? -1 : p.Offsets[1])==-1))
                        //    throw new Exception(string.Format("Partition Head Offset is -1 for partition(s): [{0}]", string.Join(",", partitions.Select(p => p.Partition + ":" + (p.Offsets.Length == 1 ? -1 : p.Offsets[1])))));

                        return new TopicPartitionOffsets(topic, partitions.ToDictionary(tp=>tp.Partition, tp=>tp.Offsets.First()));
                    }
                    catch (Exception ex)
                    {
                        retry = numAttempts < 4;
                        exception = ex;
                    }
   
                    if (retry)
                    {
                        _log.Warn("Could not fetch offsets for topic {0}. Will Retry. Message: {1}", topic, exception.Message);
                        MergeTopicMeta(await FetchMetaWithRetryAsync(topic));
                        await Task.Delay(TimeSpan.FromSeconds(1));
                    }
                    else
                    {
                        var error = "Could not fetch offsets for topic {0} after 4 attempts! Failing call";
                        _log.Fatal(exception, error, topic);
                        throw new BrokerException(error, exception);
                    }
                }
                throw new TaskCanceledException();
            }).ConfigureAwait(false);

            return ret;
        }


        #endregion Public Accessible Methods


        #region PartitionStateChanges and FetcherChanges observables

        /// <summary>
        /// Latest state of all partitions.
        /// Recovery functions can publish recoveries, sending functions can post failures,
        /// and routing functions can await for good status to come back
        /// Structure: Topic/Partition/ErrorCode.
        /// </summary>
        internal IObservable<PartitionStateChangeEvent> PartitionStateChanges { get { return _partitionStateChanges; } }
        IObservable<PartitionStateChangeEvent> _partitionStateChanges;
        private readonly ISubject<PartitionStateChangeEvent> _partitionStateChangesSubject = new Subject<PartitionStateChangeEvent>();

        /// <summary>
        /// Allow notification of a new partition state detected by a component
        /// </summary>
        /// <param name="state"></param>
        internal void NotifyPartitionStateChange(PartitionStateChangeEvent state)
        {
            _partitionStateChangesSubject.OnNext(state);
        }

        /// <summary>
        /// Compose the Subject that tracks the ready state for each topic/partiion. Always replay the most recent state for any topic/partition
        /// </summary>
        /// <returns></returns>
        private void BuildPartitionStateChangeSubject()
        {
            var conn = _partitionStateChangesSubject.
                GroupBy(t => new { t.Topic, t.PartitionId }).
                // Within partition, filter out repetition and remember the last result (for late subscribers)
                Select(p => { var conn2 = p.DistinctUntilChanged().Replay(1); conn2.Connect(); return conn2; }).
                // remember all partitions state for late subscribers
                Replay();

            conn.Connect();
            // merge per-partition unique streams back into single stream
            _partitionStateChanges = conn.Merge();

             _partitionStateChanges.Subscribe(psc => _log.Info("Cluster saw new partition state: {0}-{1}-{2}", psc.Topic, psc.PartitionId, psc.ErrorCode),
                    ex => _log.Fatal(ex, "GetFetcherChages saw ERROR from PartitionStateChanges"));
        }



        #endregion PartitionStateChanges and FetcherChanges observables

        #region NewBrokers observable

        /// <summary>
        /// Observable list of Brokers. Uses a grouped replay subject to ensure all brokers known to the cluster are returned when subscribed to.
        /// </summary>        
        internal IObservable<BrokerMeta> NewBrokers { get { return _newBrokerSubject.AsObservable(); } }
        private readonly ISubject<BrokerMeta, BrokerMeta> _newBrokerSubject;

        /// <summary>
        /// Compose a Subject that tracks each broker in the cluster. Always replay all known brokers.
        /// </summary>
        /// <returns></returns>
        private ISubject<BrokerMeta, BrokerMeta> BuildnewBrokersSubject()
        {
            var subj = new Subject<BrokerMeta>();
            var pipeline =
                // group by BrokerMeta
                subj.GroupBy(e => e).
                // select 
                Select(g =>
                {
                    // we only want one per Broker
                    var gg = g.DistinctUntilChanged().Replay(1, Scheduler);
                    gg.Connect();
                    return gg;
                }).
                Replay(Scheduler);
            pipeline.Connect();
            var pipeline2 = pipeline.Merge();
            return Subject.Create(subj, pipeline2);
        }

        #endregion NewBrokers observable

        #region Metadata Management

        internal BrokerMeta FindBrokerMetaForPartitionId(string topic, int partition)
        {
            try
            {
                var partMeta = _topicPartitionMap[topic].Single(p => p.Id == partition);
                var brokerMeta = _partitionBrokerMap[partMeta];
                return brokerMeta;
            }
            catch (KeyNotFoundException e)
            {
                _log.Error("Failed to get topic '{0}' partition {1}. Current metadata: {2}", topic, partition, _metadata);
                throw;
            }
        }

        internal async Task<Tuple<Connection, TcpClient>> GetAnyClientAsync()
        {
            // Two awaits, one for scheduler and one for Conn.GetClientAsync
            return await await Scheduler.Ask(() =>
            {
                // Query all brokers and return the 1st successful response
                var tcp = _metadata.Brokers.
                    Select(async b =>
                    {
                        var conn = b.Conn;
                        var client = await conn.GetClientAsync(noTransportErrors: true);
                        return Tuple.Create(conn, client);
                    }).
                    // replace exception with empty sequence
                    Select(t => t.ToObservable().Catch(Observable.Empty<Tuple<Connection, TcpClient>>())).
                    ToObservable().
                    Merge().
                    FirstAsync();
                return tcp;
            });
        }

        /// <summary>Get cached metadata for topic, or request, wait and cache it</summary>
        internal async Task<PartitionMeta[]> GetOrFetchMetaForTopicAsync(string topic)
        {
            CheckConnected();

            if (!_topicPartitionMap.ContainsKey(topic))
            {
                _log.Debug("Topic '{0}' does not exists. Starting pooling...", topic);
                var meta = await FetchMetaWithRetryAsync(topic);
                _log.Debug("Got topic metadata");
                MergeTopicMeta(meta);
            }

            PartitionMeta[] partMeta;
            _topicPartitionMap.TryGetValue(topic, out partMeta);
            return partMeta;
        }


        /// <summary>Will fetch topic metadata and check error status. If error
        /// might be intermitten, will keep retrying. If permanent error happen, BrokerException is thrown.</summary>
        private async Task<MetadataResponse> FetchMetaWithRetryAsync(string topic)
        {
            CheckConnected();

            // TODO: add timeout
            _log.Debug("Start topic '{0}' pooling", topic);

            while (!_cancel.IsCancellationRequested)
            {
                try
                {
                    _log.Debug("FetchMetaWithRetryAsync: sending MetadataRequest...");
                    var meta = await _protocol.MetadataRequest(new TopicRequest { Topics = new[] { topic } });
                    _log.Debug("FetchMetaWithRetryAsync: got MetadataResponse {0}", meta);
                    var errorCode = meta.Topics.Single().ErrorCode;
                    if (errorCode.IsSuccess())
                    {
                        _log.Debug("Discovered topic: '{0}'", topic);
                        return meta;
                    }

                    if (!errorCode.IsPermanentFailure())
                    {
                        _log.Debug("Topic: '{0}': LeaderNotAvailable", topic);
                        continue;
                    }

                    _log.Error("Topic: '{0}': {1}", topic, errorCode);
                    throw new BrokerException(string.Format("Can not fetch metadata for topic '{0}'. {1}", topic, errorCode));
                }
                catch (Exception e)
                {
                    if (_cancel.IsCancellationRequested) // cluster is shutting down, no big deal.
                        _log.Info(e, "Exception during shutdown while trying to fetch topic '{0}' metadata", topic);
                    else
                    {
                        // if we got a CorrelationLoopException or ObjectDisposedException, it means the connection is no longer OK. 
                        // Check if it was asked to shut down (happens a lot when "seed" connections are replaced with actual ones)
                        var cle = e as CorrelationLoopException;
                        var ode = e as ObjectDisposedException;
                        if (cle != null && cle.IsRequestedClose)
                            _log.Info(e, "Connection requested close while trying to fetch topic '{0}' metadata, will retry.", topic);
                        else if (ode != null)
                            _log.Warn(e, "Connection Disposed while trying to fetch topic '{0}' metadata, will retry.", topic);
                        else
                            _log.Error(e, "Error while trying to fetch topic '{0}' metadata, will retry.", topic);                        

                    }
                }

                await Task.Delay(500, _cancel.Token);
            }

            return null;
        } 


        private void RebuildBrokerIndexes(MetadataResponse clusterMeta = null)
        {
            // By default refresh current metadata
            if (clusterMeta == null)
                clusterMeta = _metadata;

            _topicPartitionMap = clusterMeta.Topics.ToDictionary(t => t.TopicName, t => t.Partitions);
            _partitionBrokerMap = (
                from partition in
                    (
                        from topic in clusterMeta.Topics
                        from partition in topic.Partitions
                        select partition
                    )
                group partition by partition.Leader
                    into leaderGrp
                    join brokers in clusterMeta.Brokers on leaderGrp.Key equals brokers.NodeId
                    // flatten broker->partition[] into partition->broker
                    from partition in leaderGrp
                    select new { partition, broker = brokers }
                ).ToDictionary(p => p.partition, p => p.broker);
        }

        private void MergeTopicMeta(MetadataResponse topicMeta)
        {
            // append new topics
            var newTopics = topicMeta.Topics.Except(_metadata.Topics, TopicMeta.NameComparer).ToArray();
            _metadata.Topics = _metadata.Topics.Concat(newTopics).ToArray();
            if(EtwTrace.Log.IsEnabled() && newTopics.Length > 0)
                newTopics.ForEach(t => EtwTrace.Log.MetadataNewTopic(_id, t.TopicName));

            // update existing topics
            (
                from updatedTopic in topicMeta.Topics
                where _metadata.Topics.Any(t => t.TopicName == updatedTopic.TopicName)
                // assume no new partition can happen (kafka does not allow re-partitioning)
                from oldPart in _metadata.Topics.Single(t => t.TopicName == updatedTopic.TopicName).Partitions
                from updatedPart in updatedTopic.Partitions
                where updatedPart.Id == oldPart.Id
                select new { oldPart, updatedPart, updatedTopic.TopicName }
            ).ForEach(_ =>
            {
                if (_.oldPart.ErrorCode.IsDifferent(_.updatedPart.ErrorCode))
                {
                    EtwTrace.Log.MetadataPartitionErrorChange(_id, _.TopicName, _.oldPart.Id, (int)_.oldPart.ErrorCode, (int)_.updatedPart.ErrorCode);
                    _.oldPart.ErrorCode = _.updatedPart.ErrorCode;
                }
                if (!_.oldPart.Isr.SequenceEqual(_.updatedPart.Isr))
                {
                    EtwTrace.Log.MetadataPartitionIsrChange(_id, _.TopicName, _.oldPart.Id, string.Join(",", _.oldPart.Isr), string.Join(",", _.updatedPart.Isr));
                    _.oldPart.Isr = _.updatedPart.Isr;
                }
                if (_.oldPart.Leader != _.updatedPart.Leader)
                {
                    _log.Info("Partition changed leader {0}->{1}", _.oldPart, _.updatedPart);
                    EtwTrace.Log.MetadataPartitionLeaderChange(_id, _.TopicName, _.oldPart.Id, _.oldPart.Leader, _.updatedPart.Leader);
                    _.oldPart.Leader = _.updatedPart.Leader;
                }
                if (!_.oldPart.Replicas.SequenceEqual(_.updatedPart.Replicas))
                {
                    EtwTrace.Log.MetadataPartitionReplicasChange(_id, _.TopicName, _.oldPart.Id, string.Join(",", _.oldPart.Replicas), string.Join(",", _.updatedPart.Replicas));
                    _.oldPart.Replicas = _.updatedPart.Replicas;
                }
            });

            // add new brokers
            var newBrokers = topicMeta.Brokers.Except(_metadata.Brokers, BrokerMeta.NodeIdComparer).ToArray();
            
            // Brokers which were created from seed have NodeId == -99.
            // Once we learn their true Id, update the NodeId
            var resolvedSeedBrokers = (
                from seed in _metadata.Brokers
                where seed.NodeId == -99
                from resolved in topicMeta.Brokers
                where resolved.NodeId != -99 &&
                    seed.Port == resolved.Port &&
                    string.Compare(resolved.Host, seed.Host, true, CultureInfo.InvariantCulture) == 0
                select new { seed, resolved }
            ).ToArray();

            // remove old seeds which have been resolved
            _metadata.Brokers = _metadata.Brokers.Except(resolvedSeedBrokers.Select(b => b.seed)).ToArray();

            newBrokers.ForEach(b => b.Conn = new Connection(b.Host, b.Port, e => HandleTransportError(e, b)));
            _metadata.Brokers = _metadata.Brokers.Concat(newBrokers).ToArray();

            // Close old seed connection and make sure nobody can use it anymore
            resolvedSeedBrokers.ForEach(old =>
            {
                _log.Debug("Closing seed connection because found brokerId {0} NodeId: {1}", old.seed.Conn, old.resolved.NodeId);
                old.seed.Conn.ShutdownAsync();
            });

            RebuildBrokerIndexes(_metadata);

            // broadcast any new brokers
            newBrokers.ForEach(b => EtwTrace.Log.MetadataNewBroker(_id, b.Host, b.Port, b.NodeId));
            newBrokers.Where(b => b.NodeId != -99).ForEach(b => _newBrokerSubject.OnNext(b));

            // broadcast the current partition state for all partitions.
            topicMeta.Topics.
                SelectMany(t => t.Partitions.Select(part => new PartitionStateChangeEvent(t.TopicName, part.Id, part.ErrorCode))).
                ForEach(tp => _partitionStateChangesSubject.OnNext(tp));

        }

        #endregion Metadata Management

        internal async Task<ProducerResponse> SendBatchAsync(int leader, IEnumerable<Message> batch, Producer producer)
        {
            CheckConnected();
            // TODO: do state checking. Introduce this.Connected task to wait if needed

            var request = new ProduceRequest
            {
                Broker = _metadata.Brokers.First(b => b.NodeId == leader),
                RequiredAcks = producer.Configuration.RequiredAcks,
                Timeout = producer.Configuration.ProduceRequestTimeoutMs,
                TopicData = new[] 
                {
                    new TopicData {
                        TopicName = producer.Topic,
                        PartitionsData = (
                            from msg in batch
                            // group messages belonging to the same partition
                            group msg by msg.PartitionId
                            into partitionGrp
                            select new PartitionData {
                                Pub = producer,
                                OriginalMessages = partitionGrp.ToArray(),
                                Partition = partitionGrp.Key,
                                Messages = (
                                    from msg in partitionGrp
                                    select new MessageData {
                                        Key = msg.Key,
                                        Value = msg.Value
                                    }
                                    )
                            }
                            )
                    }
                }
            };

            var response = await _protocol.Produce(request).ConfigureAwait(false);
            _log.Debug("#{0} SendBatchAsync complete", _id);
            return response;
        }

        /// <summary> 
        /// Get or create Fetcher, which is responsible for given Broker. 
        /// </summary>
        internal Fetcher GetFetcher(BrokerMeta broker, ConsumerConfiguration consumerConfig)
        {
            var fetcher = _activeFetchers.GetOrAdd(broker, b =>
            {
                var f = new Fetcher(this, b, _protocol, consumerConfig, _cancel.Token);

                // all new fetchers need to be "watched" for errors.
                // subscribe to error and complete notifications, and remove from active fetchers
                var sub = f.State.Subscribe(_ => { },
                    e => RemoveFetcher(broker, f),
                    () => RemoveFetcher(broker, f)
                );

                _shutdownDisposable.Add(sub);

                return f;
            });

            if(fetcher.ConsumerConfig != consumerConfig)
                throw new ArgumentException("Can not consume with different ConsumerConfiguration");

            return fetcher;
        }

        void RemoveFetcher(BrokerMeta broker, Fetcher fetcher)
        {
            Fetcher ef;
            var res = _activeFetchers.TryRemove(broker, out ef);
            
            if (!res)
                _log.Warn("Tried to remove fetcher {0} but not found it", fetcher);


            if (!ReferenceEquals(ef, fetcher))
            {
                _log.Error("Should not happen. Attempt to remove wrong fetcher. Tried to remove {0} but removed {1}", fetcher, ef);
                _activeFetchers.TryAdd(broker, ef);
            }
        }
    }
}
