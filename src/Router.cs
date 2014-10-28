using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using kafka4net.ConsumerImpl;
using kafka4net.Internal;
using kafka4net.Metadata;
using kafka4net.Protocols;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using kafka4net.Utils;

namespace kafka4net
{
    /// <summary>
    /// Manage connections, metadata and makes routing decisions.
    /// </summary>
    public class Router
    {
        private readonly Protocol _protocol;
        private BrokerState _state = BrokerState.Disconnected;
        private static readonly Random _rnd = new Random();
        private static readonly ILogger _log = Logger.GetLogger();

        private readonly MetadataResponse _metadata = new MetadataResponse { Brokers = new BrokerMeta[0], Topics = new TopicMeta[0]};
        
        //
        // indexed metadata
        // TODO: are kafka topics case-sensitive?
        private Dictionary<string,PartitionMeta[]> _topicPartitionMap = new Dictionary<string, PartitionMeta[]>();
        private Dictionary<PartitionMeta, BrokerMeta> _partitionBrokerMap = new Dictionary<PartitionMeta, BrokerMeta>();
        private readonly ConcurrentDictionary<BrokerMeta, Fetcher> _activeFetchers = new ConcurrentDictionary<BrokerMeta, Fetcher>();

        // Single Threaded Scheduler to handle all async methods in the library
        internal readonly EventLoopScheduler Scheduler = new EventLoopScheduler(ts => new Thread(ts) { Name = "Kafka4net-scheduler", IsBackground = true });

        // Router ID (unique number for each router instance. used in debugging messages.)
        private readonly CountObservable _inBatchCount = new CountObservable();
        private static int _idCount;
        private readonly int _id = Interlocked.Increment(ref _idCount);

        //
        // message waiting structures
        //
        private readonly Dictionary<string,List<Tuple<Publisher,Message>>> _noTopicMessageQueue = new Dictionary<string, List<Tuple<Publisher, Message>>>();
        private readonly ActionBlock<string> _topicResolutionQueue;
        private readonly  Dictionary<string,Dictionary<PartitionMeta,WaitQueueRecord>> _waitingMessages = new Dictionary<string, Dictionary<PartitionMeta, WaitQueueRecord>>();
        
        // Monitor for recovering partitions when they enter an error state.
        PartitionRecoveryMonitor _partitionRecoveryMonitor;


        readonly List<Consumer> _consumers = new List<Consumer>(); 

        /// <summary>
        /// Latest state of all partitions.
        /// Recovery functions can publish recoveries, sending functions can post failures,
        /// and routing functions can await for good status to come back
        /// Structure: Topic/Partition/ErrorCode.
        /// </summary>
        private readonly ISubject<Tuple<string, int, ErrorCode>, Tuple<string, int, ErrorCode>> _partitionStateChangesSubject;
        public IObservable<Tuple<string, int, ErrorCode>> PartitionStateChanges { get { return _partitionStateChangesSubject.AsObservable(); } } 

        /// <summary>
        /// Observable list of Brokers. 
        /// </summary>
        private readonly Subject<BrokerMeta> _newBrokerSubject = new Subject<BrokerMeta>();
        internal IObservable<BrokerMeta> NewBrokers { get { return _newBrokerSubject.AsObservable(); } } 
        
        readonly CancellationTokenSource _cancel = new CancellationTokenSource();
        private readonly Task<Task> _partitionRecoveryTask;

        /// <summary>
        /// Create broker in disconnected state. Requires ConnectAsync or ConnectAsync call.
        /// Attempt to send message in disconnected state will cause exception.
        /// </summary>
        private Router() 
        {
            // Init synchronization context of scheduler thread
            Scheduler.Schedule(() => SynchronizationContext.SetSynchronizationContext(new RxSyncContextFromScheduler(Scheduler)));

            _topicResolutionQueue = new ActionBlock<string>(t => ResolveTopic(t), 
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 5});

            // This task watches for any partitions that recover or change states.
            _partitionRecoveryTask = Task.Factory.StartNew(() => StartPartitionRecovery(), TaskCreationOptions.LongRunning);

            // build a subject that relays any changes to a partition's error state to anyone observing.
            _partitionStateChangesSubject = BuildPartitionStateChangeSubject();

            _inBatchCount.Subscribe(c => _log.Debug("#{0} InBatchCount: {1}", _id, c));
        }

        public async Task Close(TimeSpan timeout)
        {
            _log.Debug("#{0} Closing...", _id);
            _cancel.Cancel();
            _topicResolutionQueue.Complete();
            
            var success = await Task.WhenAll(new[] { 
                _partitionRecoveryMonitor.Completion, 
                _topicResolutionQueue.Completion,
                _inBatchCount.FirstAsync(c => c == 0).ToTask()
            }).TimeoutAfter(timeout);

            if (!success)
            {
                _log.Error("Timed out");
                if(!_partitionRecoveryMonitor.Completion.IsCompleted)
                    _log.Error("_partitionRecoveryMonitor timed out");
                if(!_topicResolutionQueue.Completion.IsCompleted)
                    _log.Error("_topicResolutionQueue timed out");
                var batchesInProgress = _inBatchCount.FirstAsync().ToTask().Result;
                if(batchesInProgress != 0)
                    _log.Error("There are {0} batches in progress. Timed out", batchesInProgress);
            }
            else
            {
                _log.Debug("#{0} Closed", _id);
            }
        }

        /// <summary>
        /// Compose a Subject that tracks the ready state for each topic/partiion. Always replay the most recent state for any topic/partition
        /// </summary>
        /// <returns></returns>
        private static ISubject<Tuple<string, int, ErrorCode>, Tuple<string, int, ErrorCode>> BuildPartitionStateChangeSubject()
        {
            var subj = new Subject<Tuple<string, int, ErrorCode>>();
            var pipeline = 
                // group by topic and partition
                subj.GroupBy(e => new { e.Item1, e.Item2 }).
                // select 
                Select(g =>
                {
                    // we only want the distinct groups (one per topic/partition)
                    var gg = g.DistinctUntilChanged().Replay(1); 
                    gg.Connect(); 
                    return gg;
                }).
                Replay();
            pipeline.Connect();
            var pipeline2 = pipeline.Merge();
            return Subject.Create(subj, pipeline2);
        }

        /// <summary>
        /// Gets an observable sequence of any changes to the Fetcher for a specific topic/partition
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="partitionId"></param>
        /// <param name="consumerConfiguration"></param>
        /// <returns></returns>
        internal IObservable<Fetcher> GetFetcherChanges(string topic, int partitionId, ConsumerConfiguration consumerConfiguration)
        {
            return PartitionStateChanges
                .Where(t => t.Item1 == topic && t.Item2 == partitionId)
                .Do(psc => _log.Info("GetFetcherChages saw CORRECT partition state {0}-{1}-{2}", psc.Item1, psc.Item2, psc.Item3),
                    ex => _log.Warn(ex, "GetFetcherChages saw ERROR from PartitionStateChanges"))
                .Select(state =>
                {
                    // if the state is not ready, return NULL for the fetcher.
                    if (state.Item3 != ErrorCode.NoError)
                        return (Fetcher)null;

                    // get or create the correct Fetcher for this topic/partition
                    var broker = FindBrokerMetaForPartitionId(state.Item1, state.Item2);

                    var fetcher = _activeFetchers.GetOrAdd(broker, b =>
                    {
                        // all new fetchers need to be "watched" for errors.
                        var f = new Fetcher(b, _protocol, consumerConfiguration, _cancel.Token);
                        f.FetcherPartitionErrors.Subscribe(_partitionStateChangesSubject);
                        return f;
                    });

                    return fetcher;

                })
                .Do(f=>_log.Info("GetFetcherChanges returning new fetcher {0}",f),
                    ex => _log.Error(ex, "GetFetcherChages saw ERROR returning new fetcher."),
                    ()=> _log.Error("GetFetcherChanges saw COMPLETE from returning new fetcher."));
        }


        /// <summary>
        /// ConnectAsync and fetch list of brokers and metadata.
        /// </summary>
        /// <param name="seedBrokers">Comma separated list of seed brokers. Port numbers are optional.
        /// <example>192.168.56.10,192.168.56.20:8081,broker3.local.net:8181</example>
        /// </param>
        public Router(string seedBrokers) : this()
        {
            _protocol = new Protocol(this, seedBrokers);
            _state = BrokerState.Disconnected;
        }

        #region External callable

        internal BrokerState State { get { return _state; } }

        public async Task ConnectAsync()
        {
            await await Scheduler.Ask(async () =>
            {
                if (_state != BrokerState.Disconnected)
                    return;

                _state = BrokerState.Connecting;
                var initMeta = await _protocol.ConnectAsync();
                MergeTopicMeta(initMeta);
                _state = BrokerState.Connected;

                // start up a recovery monitor to watch for recovered partitions
                _partitionRecoveryMonitor = new PartitionRecoveryMonitor(this, _protocol, _cancel.Token);
                // Merge metadata that recovery monitor discovers
                _partitionRecoveryMonitor.NewMetadataEvents.Subscribe(MergeTopicMeta, ex => _log.Error(ex, "Error thrown by RecoveryMonitor.NewMetadataEvents!"));
            });
        }

        /// <summary>Get topics which are already cached. Does not issue metadata request.</summary>
        public Task<TopicMeta[]> GetTopics()
        {
            if(_state != BrokerState.Connected)
                throw new BrokerException("Broker is not connected");

            return Scheduler.Ask(() => _metadata.Topics);
        }

        /// <summary>
        /// Get the cached partition metadata for the given topic if it exists, otherwise issue a topic metadata request to get it
        /// </summary>
        /// <param name="topic"></param>
        /// <returns></returns>
        public async Task<PartitionMeta[]> GetTopicPartitionsAsync(string topic)
        {
            PartitionMeta[] partitions;

            if (_topicPartitionMap.TryGetValue(topic, out partitions))
                return partitions;

            await GetOrFetchMetaForTopic(topic);

            _topicPartitionMap.TryGetValue(topic, out partitions);

            return partitions;

        }

        public async Task<PartitionInfo[]> GetPartitionsInfo(string topic)
        {
            var ret = await await Scheduler.Ask(async () => {
                // get partition list
                await GetOrFetchMetaForTopic(topic);
                var parts = _topicPartitionMap[topic].
                    Select(p => new OffsetRequest.PartitionData {
                        Id = p.Id,
                        Time = (long)ConsumerStartLocation.TopicTail
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

                // TODO: handle recoverable errors, such as tcp transport exceptions (with limited retry)
                // or partition relocation
                // TODO: handler error codes
                if(requests.Any(r => r.IsFaulted))
                    throw new AggregateException("Failure when getting offsets info", requests.Where(r => r.IsFaulted).Select(r => r.Exception));

                return (
                    from r in requests
                    from part in r.Result.Partitions
                    select new PartitionInfo { 
                        Partition = part.Partition, 
                        Head = part.Offsets.Length == 1 ? -1 : part.Offsets[1], 
                        Tail = part.Offsets[0] 
                    }
                ).ToArray();
            });

            return ret;
        }

        /// <summary>Get cached metadata for topic, or request, wait and cache it</summary>
        private async Task GetOrFetchMetaForTopic(string topic)
        {
            if (!_topicPartitionMap.ContainsKey(topic))
            {
                _log.Debug("Topic '{0}' does not exists. Starting pooling...", topic);
                var meta = await PoolMeta(topic);
                _log.Debug("Got topic metadata");
                MergeTopicMeta(meta);
            }
        }


        #endregion

        /// <summary>Will fetch topic metadata and check error status. If error
        /// might be intermitten, will keep retrying. If permanent error happen, BrokerException is thrown.</summary>
        private async Task<MetadataResponse> PoolMeta(string topic)
        {
            // TODO: add timeout
            _log.Debug("Start topic '{0}' pooling", topic);

            while (!_cancel.IsCancellationRequested)
            {
                try
                {
                    _log.Debug("PoolMeta: sending MetadataRequest...");
                    var meta = await _protocol.MetadataRequest(new TopicRequest { Topics = new[] { topic } });
                    _log.Debug("PoolMeta: got MetadataResponse {0}", meta);
                    var errorCode = meta.Topics.Single().TopicErrorCode;
                    switch (errorCode)
                    {
                        case ErrorCode.NoError:
                            _log.Debug("Discovered topic: '{0}'", topic);
                            return meta;
                        case ErrorCode.LeaderNotAvailable:
                            _log.Debug("Topic: '{0}': LeaderNotAvailable", topic);
                            break;
                        default:
                            _log.Error("Topic: '{0}': {1}", topic, errorCode);
                            throw new BrokerException(string.Format("Can not fetch metadata for topic '{0}'. {1}", topic, errorCode));
                    }
                }
                catch (Exception e)
                {
                    _log.Error("Error while trying to fetch topic '{0}' metadata. {1}", topic, e.Message);
                }

                await Task.Delay(500, _cancel.Token);
            }

            return null;
        } 

        BrokerMeta FindBrokerMetaForPartitionId(string topic, int partition)
        {
            // TODO: how to handle not found exceptions, downed partitions?
            var leader = _topicPartitionMap[topic].
                Single(p => p.Id == partition).Leader;
            var broker = _metadata.Brokers.Single(b => b.NodeId == leader);
            _log.Debug("FindBrokerMetaForPartitionId '{0}'/{1} -> {2}", topic, partition, broker);
            return broker;
        }

        internal async Task SendBatch(Publisher publisher, IList<Message> batch)
        {
            _inBatchCount.Incr();
            try
            {
                _log.Debug("#{0} SendBatch messages: {1}", _id, batch.Count);

                if (_cancel.IsCancellationRequested)
                    throw new BrokerException("Can not send, router is closed");

                if (_state == BrokerState.Connecting || _state == BrokerState.Disconnected)
                {
                    // group by publisher to send errors in batches
                    //foreach (var pubGroup in batch.GroupBy(_ => _.Item1))
                    //{
                    //    var publisher = pubGroup.Key;
                    switch (_state)
                    {
                        case BrokerState.Disconnected:
                            // TODO: make OnPermError accepting IList or IEnumerable?
                            publisher.OnPermError(new BrokerException("Router is not connected"), batch.ToArray());
                            break;
                        case BrokerState.Connecting:
                            // TODO:
                            //pubGroup.ToList().ForEach(_delayedMessages.Enqueue);
                            break;
                    }
                    //}
                    _log.Debug("SendBatch complete");
                    return;
                }

                if (_state == BrokerState.Connected)
                {
                    // BNF:
                    // ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
                    //      RequiredAcks => int16
                    //      Timeout => int32
                    //      Partition => int32
                    //      MessageSetSize => int32
                    var waitingList = new List<Tuple<Publisher, PartitionMeta, Message>>();
                    var requests = (
                        from msg in
                            (
                                // extend message with broker and partition info
                                from msg in batch
                                let brokerPart = FindBrokerAndPartition(msg, publisher, waitingList)
                                // Messages without known partition are sent to retry by FindBrokerAndPartition()
                                // so just skip them here
                                where brokerPart != null
                                select new { Msg = msg, Broker = brokerPart.Item1, Part = brokerPart.Item2 }
                                )
                        // Group by broker to send one batch per physical connection
                        group msg by msg.Broker
                            into routeGrp
                            select new ProduceRequest
                            {
                                Broker = routeGrp.Key,
                                RequiredAcks = publisher.Acks,
                                Timeout = publisher.TimeoutMs,
                                TopicData = new[] 
                                {
                                    new TopicData {
                                        TopicName = publisher.Topic,
                                        PartitionsData = (
                                            from msg in routeGrp
                                            // group messages belonging to the same partition
                                            group msg by msg.Part
                                            into partitionGrp
                                            select new PartitionData {
                                                Pub = publisher,
                                                OriginalMessages = partitionGrp.Select(m => m.Msg).ToArray(),
                                                Partition = partitionGrp.Key.Id,
                                                Messages = (
                                                    from msg in partitionGrp
                                                    select new MessageData {
                                                        Key = msg.Msg.Key,
                                                        Value = msg.Msg.Value
                                                    }
                                                )
                                            }
                                        )
                                    }
                                }
                            }
                    ).ToArray(); // materialize to fill out waitingList

                    PutMessagesIntoWaitingQueue(waitingList);

                    await _protocol.Produce(requests);
                    _log.Debug("#{0} SendBatch complete", _id);
                    return;
                }

                throw new Exception("Unknown state: " + _state);
            }
            finally {
                _inBatchCount.Decr();
            }
        }

        /// <summary>Find partition and connection for given message key/topic.
        /// If topic meta does not exist, send message to the topic resolution queue
        /// </summary>
        /// <param name="waitingList">If partition is not found, null will be returned but message is enqueued onto this list for further resolution.</param>
        /// <returns>Null if metadata for this topic not found</returns>
        private Tuple<BrokerMeta,PartitionMeta> FindBrokerAndPartition(Message msg, Publisher pub, List<Tuple<Publisher,PartitionMeta,Message>> waitingList)
        {
            PartitionMeta[] parts;
            var topic = pub.Topic;

            if (!_topicPartitionMap.TryGetValue(topic, out parts))
            {
                EnqueueToTopicResolutionQueue(msg, pub, topic);
                return null;
            }

            var index = msg.Key == null ?
                _rnd.Next(parts.Length) :
                Fletcher32HashOptimized(msg.Key) % parts.Length;

            var part = parts[index];
            var broker = _partitionBrokerMap[part];
            
            if (broker.Conn.State == ConnState.Connecting || part.ErrorCode == ErrorCode.LeaderNotAvailable)
            {
                waitingList.Add(Tuple.Create(pub, part, msg));
                return null;
            }

            return Tuple.Create(broker, part);
        }

        /// <summary>Optimized Fletcher32 checksum implementation.
        /// <see cref="http://en.wikipedia.org/wiki/Fletcher%27s_checksum#Fletcher-32"/></summary>
        private uint Fletcher32HashOptimized(byte[] msg)
        {
            if (msg == null)
                return 0;
            var words = msg.Length;
            int i = 0;
            uint sum1 = 0xffff, sum2 = 0xffff;

            while(words != 0)
            {
                var tlen = words > 359 ? 359 : words;
                words -= tlen;
                do
                {
                    sum2 += sum1 += msg[i++];
                } while (--tlen != 0);
                sum1 = (sum1 & 0xffff) + (sum1 >> 16);
                sum2 = (sum2 & 0xffff) + (sum2 >> 16);
            }
            /* Second reduction step to reduce sums to 16 bits */
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
            return (sum2 << 16 | sum1);
        }

        void RebuildBrokerIndexes(MetadataResponse clusterMeta = null)
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
                join brokerConn in
                (
                    from broker in clusterMeta.Brokers
                    select new {broker, conn = new Connection(broker.Host, broker.Port, _protocol)}
                ) on leaderGrp.Key equals brokerConn.broker.NodeId
                // flatten broker->partition[] into partition->broker
                from partition in leaderGrp
                select new {partition, broker = brokerConn}
                ).ToDictionary(p => p.partition, p =>
                {
                    p.broker.broker.Conn = p.broker.conn;
                    return p.broker.broker;
                });
        }

        private void MergeTopicMeta(MetadataResponse topicMeta)
        {
            // append new topics
            var newTopics = topicMeta.Topics.Except(_metadata.Topics, TopicMeta.NameComparer);
            _metadata.Topics = _metadata.Topics.Concat(newTopics).ToArray();
            
            // update existing topics
            (
                from updatedTopic in topicMeta.Topics
                where _metadata.Topics.Any(t => t.TopicName == updatedTopic.TopicName)
                // assume no new partition can happen (kafka does not allow re-partitioning)
                from oldPart in _metadata.Topics.Single(t => t.TopicName == updatedTopic.TopicName).Partitions
                from updatedPart in updatedTopic.Partitions
                where updatedPart.Id == oldPart.Id
                select new { oldPart, updatedPart}
            ).ForEach(_ => {
                if (_.oldPart.ErrorCode != _.updatedPart.ErrorCode)
                    _.oldPart.ErrorCode = _.updatedPart.ErrorCode;
                if (!_.oldPart.Isr.SequenceEqual(_.updatedPart.Isr))
                    _.oldPart.Isr = _.updatedPart.Isr;
                if (_.oldPart.Leader != _.updatedPart.Leader)
                {
                    _log.Info("Partition changed leader {0}->{1}", _.oldPart, _.updatedPart);
                    _.oldPart.Leader = _.updatedPart.Leader;
                }
                if (!_.oldPart.Replicas.SequenceEqual(_.updatedPart.Replicas))
                    _.oldPart.Replicas = _.updatedPart.Replicas;
            });

            // add new brokers
            var newBrokers = topicMeta.Brokers.Except(_metadata.Brokers, BrokerMeta.NodeIdComparer).ToArray();
            _metadata.Brokers = _metadata.Brokers.Concat(newBrokers).ToArray();

            RebuildBrokerIndexes(_metadata);

            // broadcast any new brokers
            newBrokers.ForEach(b => _newBrokerSubject.OnNext(b));

            // broadcast the current partition state for all partitions.
            topicMeta.Topics.SelectMany(t=>t.Partitions.Select(part=>new Tuple<string,int,ErrorCode>(t.TopicName,part.Id,part.ErrorCode))).ForEach(tp=>_partitionStateChangesSubject.OnNext(tp));
            
        }

        // TODO: is called from ActionBlock but acess dictionaries. Unsafe!!!
        // TODO: unify with partition recovery flow
        async Task ResolveTopic(string topic)
        {
            var req = new TopicRequest { Topics = new[] { topic } };
            try 
            {
resend:
                var topicMeta = await _protocol.MetadataRequest(req);
                switch (topicMeta.Topics[0].TopicErrorCode)
                {
                    case ErrorCode.NoError:
                        Scheduler.Schedule(async () => { 
                            var delayedMessages = _noTopicMessageQueue[topic];
                            _noTopicMessageQueue.Remove(topic);
                            MergeTopicMeta(topicMeta);
                            // Issue out-of timer SendBatch to catch up all
                            // TODO: make sure SendBatch from the main data stream is 
                            // handled in kafka-broker thread
                            foreach(var delayedMessage in delayedMessages.GroupBy(t => t.Item1, t => t.Item2))
                                await SendBatch(delayedMessage.Key, delayedMessage.ToArray());
                            // TODO: how to *prepend* messages to the queue?
                        });
                        break;
                    case ErrorCode.LeaderNotAvailable:
                        // TODO: limit retry by time
                        while (true)
                        {
                            _log.Info("Leader not available for '{0}'. Will retry", topic);
                            // TODO: progressive increase time of wait from 50ms..15sec
                            await Task.Delay(TimeSpan.FromSeconds(1));
                            goto resend;
                        }
                    default:
                        var error = string.Format("Topic '{0}' can not be resolved. Error: {1}", topic, topicMeta.Topics[0].TopicErrorCode);
                        throw new BrokerException(error);
                }
            }
            catch (Exception e)
            {
                Scheduler.Schedule(() => {
                    var failedMessages = _noTopicMessageQueue[topic];
                    _noTopicMessageQueue.Remove(topic);
                    failedMessages.
                        GroupBy(m => m.Item1).
                        Where(g => g.Key.OnPermError != null).
                        ForEach(g => g.Key.OnPermError(e, g.Select(t => t.Item2).ToArray()));
                });
            }
        }

        internal void EnqueueToTopicResolutionQueue(Message msg, Publisher pub, string topic)
        {
            Scheduler.Schedule(() =>
            {
                if (!_noTopicMessageQueue.ContainsKey(topic))
                {
                    _noTopicMessageQueue.Add(topic, new List<Tuple<Publisher,Message>> { Tuple.Create(pub, msg) });
                    _topicResolutionQueue.Post(topic);
                }
                else
                {
                    _noTopicMessageQueue[topic].Add(Tuple.Create(pub, msg));
                }
            });
        }

        internal async Task<TcpClient> GetAnyClient()
        {
            // Two awaits, one for scheduler and one for Conn.GetClient
            return await await Scheduler.Ask(() =>
            {
                // TODO: would it be a good idea to query all brokers and return the 1st successful response?
                // This way we do not fail the whole driver if first broker is down
                var tcp = _metadata.Brokers.
                    Select(b => b.Conn.GetClient()).
                    First();
                return tcp;
            });
        }

        /// <summary> 
        /// In case of transport error, consider it temporary.
        /// Mark broker as broken, start connection pooling and enqueue messages
        /// into waiting queue.
        /// </summary>
        internal void OnTransportError(ProduceRequest request, SocketException e)
        {
            Scheduler.Schedule(() =>
            {
                // start connection pooling only 1st time
                if (request.Broker.Conn.State == ConnState.Connected)
                {
                    _log.Debug("Switching broker into Connecting mode {0}", request.Broker);
                    request.Broker.Conn.State = ConnState.Connecting;
                    
                    // set partition state to leader unavailable
                    (
                        from topic in request.TopicData
                        from partMeta in _topicPartitionMap[topic.TopicName]
                        select partMeta
                    ).ForEach(p => p.ErrorCode = ErrorCode.LeaderNotAvailable);
                    
                    PutMessagesIntoWaitingQueue(request);
                    FireTempError(request);
                }
                else if (request.Broker.Conn.State == ConnState.Connecting)
                {
                    // just add messages to the waiting queue, connection pooling is already started
                    PutMessagesIntoWaitingQueue(request);
                    FireTempError(request);
                }
                else
                {
                    // connection failed permanently, no sending is possible
                    (
                        from topic in request.TopicData
                        from part in topic.PartitionsData
                        where part.Pub.OnPermError != null
                        select part
                    ).ForEach(part => part.Pub.OnPermError(e, part.OriginalMessages));
                }

            });
        }

        private void PutMessagesIntoWaitingQueue(ProduceRequest request)
        {
            (
                from topic in request.TopicData
                from part in topic.PartitionsData
                select new {topic.TopicName, part}
            ).ForEach(msgs =>
            {
                Dictionary<PartitionMeta, WaitQueueRecord> part;
                if(!_waitingMessages.TryGetValue(msgs.TopicName, out part))
                    _waitingMessages.Add(msgs.TopicName, part = new Dictionary<PartitionMeta, WaitQueueRecord>());
                WaitQueueRecord waitingMsgs;
                var partMeta = _topicPartitionMap[msgs.TopicName].Single(p => p.Id == msgs.part.Partition);
                if(!part.TryGetValue(partMeta, out waitingMsgs))
                    part.Add(partMeta, waitingMsgs = new WaitQueueRecord());
                waitingMsgs.Messages.AddRange(msgs.part.OriginalMessages);
                waitingMsgs.Pub = msgs.part.Pub;
            });
        }

        private void PutMessagesIntoWaitingQueue(List<Tuple<Publisher,PartitionMeta,Message>> messages)
        {
            (
                from message in messages
                group message by new { message.Item2, message.Item1 }
                    into msgGrp
                    select new {Pub = msgGrp.Key.Item1, Part = msgGrp.Key.Item2, Messages = msgGrp.Select(m => m.Item3).ToArray()}

            ).ForEach(msgGrp => {
                Dictionary<PartitionMeta, WaitQueueRecord> part;
                if (!_waitingMessages.TryGetValue(msgGrp.Pub.Topic, out part))
                    _waitingMessages.Add(msgGrp.Pub.Topic, part = new Dictionary<PartitionMeta, WaitQueueRecord>());
                WaitQueueRecord waitingMsgs;
                var partMeta = msgGrp.Part;
                if (!part.TryGetValue(partMeta, out waitingMsgs))
                    part.Add(partMeta, waitingMsgs = new WaitQueueRecord());
                waitingMsgs.Messages.AddRange(msgGrp.Messages);
                waitingMsgs.Pub = msgGrp.Pub;            
            });
        }


        private void FireTempError(ProduceRequest request)
        {
            (from topic in request.TopicData
             from part in topic.PartitionsData
             where part.Pub.OnTempError != null
             select new {part.Pub, part.OriginalMessages}
            ).ForEach(p => p.Pub.OnTempError(p.OriginalMessages));
        }

        async Task StartPartitionRecovery()
        {
            //
            // Query all brokers in parallel. 
            // Fetch metadata from first broker which responds.
            // Re-fetch metadata from failed partition leader to make sure it is alive (unless it is the same just queried).

            while (!_cancel.IsCancellationRequested)
            {
                var brokers = await Scheduler.Ask(() => _metadata.Brokers.ToArray());

                foreach (var broker in brokers)
                {
                    // TODO: fail upon certain timeout
                    try
                    {
                        // any topic which has partitions in non-draining state. Draining can stay in the queue for some time
                        var topics = await Scheduler.Ask(() => _waitingMessages.Where(kv => kv.Value.Any(p => p.Key.ErrorCode != ErrorCode.Draining)).
                            Select(kv => kv.Key).
                            ToArray());
                        if (topics.Length == 0)
                        {
                            await Task.Delay(1000);
                            // TODO: we can complete this task, but how to avoid race condition
                            // between adding new waiting messages and task completion?
                            break;
                        }

                        _log.Info("Starting partition recovery. Broker: {0}", broker);

                        var metaNew = await _protocol.MetadataRequest(new TopicRequest { Topics = topics }, broker);
                        // if no exception, broker is back online. TODO: this is not thread-safe
                        if(broker.Conn.State == ConnState.Connecting)
                            broker.Conn.State = ConnState.Connected;

                        Scheduler.Schedule(() =>
                        {
                            // find healed partitions, if any
                            foreach (var metaTopicNew in metaNew.Topics.Where(t => t.TopicErrorCode == ErrorCode.NoError))
                            {
                                Dictionary<PartitionMeta, WaitQueueRecord> waitingParts;
                                if (!_waitingMessages.TryGetValue(metaTopicNew.TopicName, out waitingParts))
                                    continue;
                                foreach (var metaPartSuccessNew in metaTopicNew.Partitions.Where(p => p.ErrorCode == ErrorCode.NoError))
                                {
                                    var waitingPart = waitingParts.Keys.SingleOrDefault(p => p.Id == metaPartSuccessNew.Id);
                                    if (waitingPart == null)
                                        continue;

                                    // update meta of healed partitions
                                    _log.Info("Recovered partition {0}->{1}", waitingPart, metaPartSuccessNew);
                                    waitingPart.ErrorCode = ErrorCode.Draining;
                                    waitingPart.Isr = metaPartSuccessNew.Isr;
                                    waitingPart.Leader = metaPartSuccessNew.Leader;
                                    waitingPart.Replicas = metaPartSuccessNew.Replicas;

                                    if (waitingParts.Count == 0)
                                    {
                                        _waitingMessages.Remove(metaTopicNew.TopicName);
                                        _log.Info("Topic '{0}' recovered", metaTopicNew.TopicName);
                                    }

                                    RebuildBrokerIndexes();
                                }
                            }

                            // TODO: find unrecoverable errors and fail messages permanently

                            RescanWaitingQueue();
                        });


                    }
                    catch (Exception e)
                    {
                        _log.Debug("Recovery loop error: ", e.Message);
                    }
                    // TODO: progressive increase delay
                    await Task.Delay(TimeSpan.FromSeconds(3));
                }
            }
        }

        private void RescanWaitingQueue()
        {
            // TODO: how to make sure this function is not called simulteounesly?
            Scheduler.Schedule(async () => {
                var recoveredParts = (from tkv in _waitingMessages
                 from pkv in tkv.Value
                 where pkv.Key.ErrorCode == ErrorCode.Draining
                 select new { Topic = tkv.Key, Part = pkv.Key, pkv.Value.Pub, pkv.Value.Messages }).ToArray();

                // resend
                foreach(var recovered in recoveredParts) {
                    var broker = _partitionBrokerMap[recovered.Part];
                    var topicData = new TopicData {
                        TopicName = recovered.Topic, 
                        PartitionsData = new[]  {
                            new PartitionData {
                                Pub = recovered.Pub,
                                Partition = recovered.Part.Id,
                                OriginalMessages = recovered.Messages.ToArray(),
                                Messages = recovered.Messages.Select(m => new MessageData {
                                    Key = m.Key,
                                    Value = m.Value
                                }).ToArray()
                            } 
                        }
                    };
                    var response = await _protocol.ProduceRaw(new ProduceRequest {Broker = broker, RequiredAcks = recovered.Pub.Acks, Timeout = recovered.Pub.TimeoutMs, TopicData = new[] {topicData}}, CancellationToken.None);

                    var errorCode = response.Topics.Single().Partitions.Single().ErrorCode;
                    if (errorCode == ErrorCode.NoError)
                    {
                        var recovered1 = recovered;  // just make compiler happy
                        Scheduler.Schedule(() => {
                            // remove from waiting
                            _waitingMessages[recovered1.Topic].Remove(recovered1.Part);
                        });

                        if (recovered.Pub.OnSuccess != null)
                            recovered.Pub.OnSuccess(recovered.Messages.ToArray());

                        _log.Info("Resent success. Messages: {0}, Topic: '{1}', Broker: {2}, Part: {3}", recovered1.Messages.Count, recovered1.Topic, broker, recovered.Part.Id);
                    }
                    else
                    {
                        // do nothing, messages will remain in the waiting queue
                        _log.Error("Failed to resend messages from waiting queue. Error: {0}, broker: {1}, messages: {2}", errorCode, broker, recovered.Messages.Count);
                    }
                }
            });
        }

        internal enum BrokerState
        {
            Disconnected,
            Connecting,
            Connected
        }

        class WaitQueueRecord {
            public List<Message> Messages = new List<Message>();
            public Publisher Pub;
        }

        #if DEBUG
        internal BrokerMeta TestGetBrokerForPartition(string topic, int partition)
        {
            var partMeta = _topicPartitionMap[topic].Single(p => p.Id == partition);
            var brokerMeta = _partitionBrokerMap[partMeta];
            return brokerMeta;
        }
        #endif

        public string[] GetAllTopics()
        {
            if(_metadata == null)
                return new string[0];
            return Scheduler.Ask(() => _metadata.Topics.Select(t => t.TopicName).ToArray()).Result;
        }

    }
}
