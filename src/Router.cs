using System;
using System.Collections.Generic;
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
using kafka4net.Protocol;
using kafka4net.Protocol.Requests;
using kafka4net.Protocol.Responses;
using kafka4net.Utils;

namespace kafka4net
{
    /// <summary>
    /// Manage connections, metadata and makes routing decisions.
    /// </summary>
    public class Router
    {
        readonly Transport _connection;
        BrokerState _state = BrokerState.Disconnected;
        static readonly Random _rnd = new Random();
        static readonly ILogger _log = Logger.GetLogger();

        readonly MetadataResponse _metadata = new MetadataResponse { Brokers = new BrokerMeta[0], Topics = new TopicMeta[0]};
        // indexed metadata
        // TODO: are kafka topics case-sensitive?
        Dictionary<string,PartitionMeta[]> _topicPartitionMap = new Dictionary<string, PartitionMeta[]>();
        Dictionary<PartitionMeta, BrokerMeta> _partitionBrokerMap = new Dictionary<PartitionMeta, BrokerMeta>();
        readonly EventLoopScheduler _scheduler = new EventLoopScheduler(ts => new Thread(ts) { Name = "Kafka-route" });
        CountObservable _inBatchCount = new CountObservable();
        static int _idCount;
        readonly int _id = Interlocked.Increment(ref _idCount);

        //
        // message waiting structures
        //
        readonly Dictionary<string,List<Tuple<Publisher,Message>>> _noTopicMessageQueue = new Dictionary<string, List<Tuple<Publisher, Message>>>();
        readonly ActionBlock<string> _topicResolutionQueue;
        readonly  Dictionary<string,Dictionary<PartitionMeta,WaitQueueRecord>> _waitingMessages = new Dictionary<string, Dictionary<PartitionMeta, WaitQueueRecord>>();
        PartitionRecoveryMonitor _partitionRecoveryMonitor;

        readonly List<Fetcher> _deadFetchers = new List<Fetcher>(); 
        readonly List<Fetcher> _activeFetchers = new List<Fetcher>();

        readonly List<Consumer> _consumers = new List<Consumer>(); 

        /// <summary>
        /// Latest state of all partitions.
        /// Recovery functions can publish recoveries, sending functions can post failures,
        /// and routing functions can await for good status to come back
        /// Structure: Topic/Partition/IsReady.
        /// </summary>
        readonly ISubject<Tuple<string, int, bool>, Tuple<string, int, bool>> _partitionReadyEvents;
        
        readonly CancellationTokenSource _cancel = new CancellationTokenSource();
        private Task<Task> _partitionRecoveryTask;

        /// <summary>
        /// Create broker in disconnected state. Requires ConnectAsync or ConnectAsync call.
        /// Attempt to send message in disconnected state will cause exception.
        /// </summary>
        public Router() 
        {
            _topicResolutionQueue = new ActionBlock<string>(t => ResolveTopic(t), 
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = 5});

            _partitionRecoveryTask = Task.Factory.StartNew(() => StartPartitionRecovery(), TaskCreationOptions.LongRunning);

            _partitionReadyEvents = BuildPartitionReadyEvent();

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

        private void StartPartitionMonitor()
        {
            _partitionRecoveryMonitor = new PartitionRecoveryMonitor(_metadata.Brokers, _connection, _cancel.Token);
            // Merge metadata that recovery monitor discovers
            _partitionRecoveryMonitor.NewMetadataEvents.Subscribe(meta => _scheduler.Schedule(() => MergeTopicMeta(meta)));
            _partitionRecoveryMonitor.RecoveryEvents.Subscribe(
                parts => OnPartitionRecoveredUpdateConsumers(parts.Item1, parts.Item2));
        }

        static ISubject<Tuple<string, int, bool>, Tuple<string, int, bool>> BuildPartitionReadyEvent()
        {
            var subj = new Subject<Tuple<string, int, bool>>();
            var pipeline = subj.GroupBy(e => e.Item1).
                Select(g => { var gg = g.DistinctUntilChanged().Replay(1); gg.Connect(); return gg; }).
                Replay();
            pipeline.Connect();
            var pipeline2 = pipeline.Merge();
            return Subject.Create(subj, pipeline2);
        }

        /// <summary>
        /// ConnectAsync and fetch list of brokers and metadata.
        /// </summary>
        /// <param name="seedBrokers">Comma separated list of seed brokers. Port numbers are optional.
        /// <example>192.168.56.10,192.168.56.20:8081,broker3.local.net:8181</example>
        /// </param>
        public Router(string seedBrokers) : this()
        {
            _connection = new Transport(this, seedBrokers);
            _state = BrokerState.Disconnected;
        }

        #region External callable

        public async Task ConnectAsync()
        {
            await await _scheduler.Ask(async () =>
            {
                if (_state != BrokerState.Disconnected)
                    return;

                _state = BrokerState.Connecting;
                var initMeta = await _connection.ConnectAsync();
                MergeTopicMeta(initMeta);
                _state = BrokerState.Connected;
                StartPartitionMonitor();
            });
        }

        /// <summary>Get topics which are already cached. Does not issue metadata request.</summary>
        public Task<TopicMeta[]> GetTopics()
        {
            if(_state != BrokerState.Connected)
                throw new BrokerException("Broker is not connected");

            return _scheduler.Ask(() => _metadata.Topics);
        }

        /// <summary>Resolve initial offsets of partitions and start fetching loop</summary>
        internal async Task<IObservable<ReceivedMessage>> InitFetching(Consumer consumer)
        {
            // TODO: synchronization!

            // TODO: implement consumer removal when unsubscribed
            _consumers.Add(consumer);

            // If topic meta is missing, try to get it. It could be created externally since the last check
            // or topic autocreate feature is ON and new topic will appear shortly.
            if (!_topicPartitionMap.ContainsKey(consumer.Topic))
            {
                var meta = await PoolMeta(consumer.Topic);
                MergeTopicMeta(meta);
            }

            // Got metadata, now calculate routungs of partition to broker, group by broker
            // and add the goup of partitions to appropriate Fetcher
            return (
                from part in _topicPartitionMap[consumer.Topic]
                let broker = FindBrokerMetaForPartitionId(consumer.Topic, part.Id)
                // group partitions belonging to the same broker into single batch
                group part.Id by broker into brokerGrp
                select brokerGrp
                // issue request 
                // TODO: relaiability. If offset failed, try to recover N times
            ).Select(async listeningPartitions =>
            {
                var consumerKey = Tuple.Create(listeningPartitions.Key.NodeId, consumer.MaxWaitTimeMs, consumer.MinBytes);
                var fetcher = _activeFetchers.Concat(_deadFetchers).
                    FirstOrDefault(f => f.Key.Equals(consumerKey));
                if (fetcher == null)
                {
                    fetcher = new Fetcher(listeningPartitions.Key, _connection, consumer, _cancel.Token);
                    _activeFetchers.Add(fetcher);
                }
                
                await fetcher.ResolveTime(consumer, listeningPartitions.ToArray());
                
                return fetcher.AsObservable().
                    SelectMany(msg => msg.Topics).
                    Where(t => t.Topic == consumer.Topic).
                    Do(_ => {}, e => AddFetcherMonitoring(fetcher, e)).
                    SelectMany(topic => {
                        _log.Debug("#{0} Received fetch message", _id);
                        return (
                            from part in topic.Partitions
                            from msg in part.Messages
                            select new ReceivedMessage
                            {
                                Topic = topic.Topic,
                                Partition = part.Partition,
                                Key = msg.Key,
                                Value = msg.Value
                            });
                    });
            }).
            // TODO: could this be avoided by rewriting the body somehow???
            ToObservable().Merge().Merge();
        }

        void AddFetcherMonitoring(Fetcher fetcher, Exception e)
        {
            // in case of failure, start partition recovery process
            _activeFetchers.Remove(fetcher);
            _partitionRecoveryMonitor.StartRecovery(fetcher.GetOffsetStates());
            _log.Info("Fetcher failed. Started recovery.", e);
        }

        /// <summary>After partitions are recoverd, Leader may change. Moreover, now aprtitions may belong to different leaders,
        /// which requires re-grouping them according to new Leader information</summary>
        void OnPartitionRecoveredUpdateConsumers(string recoveryTopic, IEnumerable<PartitionFetchState> recoveryStates)
        {
            foreach (var consumer in _consumers.Where(c => c.Topic == recoveryTopic))
            {
                _log.Debug("Detected recovered partitions. Topic: {0}, partitions: {1}", recoveryTopic, string.Join(",", recoveryStates));

                bool needSubscription = false;
                Fetcher fetcher;

                var brokerIdAndParts = (
                    from partState in recoveryStates
                    let leader = _topicPartitionMap[recoveryTopic].Single(p => p.Id == partState.PartId).Leader
                    group partState by leader into brokerGrp
                    select new { BrokerId = brokerGrp.Key, Parts = brokerGrp.ToList()}
                );

                foreach (var brokerGroup in brokerIdAndParts)
                {
                    fetcher = _activeFetchers.SingleOrDefault(f => f.BrokerId == brokerGroup.BrokerId);
                    if (fetcher != null)
                    {
                        if (!fetcher.HasConsumer(consumer))
                        { // there is a fetcher but consumer is not subscribed
                            needSubscription = true;
                            _log.Debug("Fetcher already exists {0} but Consumer is not subscribed. Will subscribe", fetcher);
                        }
                        else
                        {
                            _log.Error("Fetcher already exists and Consumer is already subscribed {0}. Should not happen!", fetcher);
                        }
                    }
                    else
                    {
                        // no fetcher for this partition group
                        var broker = _metadata.Brokers.Single(b => b.NodeId == brokerGroup.BrokerId);
                        var fetcher2 = new Fetcher(broker, _connection, consumer, brokerGroup.Parts, _cancel.Token);
                        fetcher2.AsObservable().Subscribe(_ => { }, e => AddFetcherMonitoring(fetcher2, e));
                        fetcher = fetcher2;
                        needSubscription = true;
                        _log.Debug("No Fetcher exists. Fetcher is created on {0} and Consumer will subscribe", broker);
                    }

                    //
                    // subscribe before merging new partitions to avoid events loss
                    //
                    if (needSubscription)
                    {
                        var recoveredPartitions = fetcher.
                            AsObservable().
                            SelectMany(f => f.Topics).
                            Where(t => t.Topic == recoveryTopic);
                        consumer.OnPartitionsRecovered(recoveredPartitions);
                        _log.Debug("Subscribed Consumer to fetcher: {0} on topic {1}", fetcher, recoveryTopic);
                    }

                    fetcher.AdToListeningPartitions(consumer, brokerGroup.Parts);
                }
            }
        }

        #endregion

        /// <summary>Will fetch topic metadata and check error status. If error
        /// might be intermitten, will weep retrying.</summary>
        async Task<MetadataResponse> PoolMeta(string topic)
        {
            // TODO: add timeout
            while (!_cancel.IsCancellationRequested)
            {
                try
                {
                    var meta = await _connection.MetadataRequest(new TopicRequest { Topics = new[] { topic } });
                    var errorCode = meta.Topics.Single().TopicErrorCode;
                    switch (errorCode)
                    {
                        case ErrorCode.NoError:
                            return meta;
                        case ErrorCode.LeaderNotAvailable:
                            break;
                        default:
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

                    await _connection.Produce(requests);
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
                    select new {broker, conn = new Connection(broker.Host, broker.Port, _connection)}
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
            var newBrokers = topicMeta.Brokers.Except(_metadata.Brokers, BrokerMeta.NodeIdComparer);
            _metadata.Brokers = _metadata.Brokers.Concat(newBrokers).ToArray();

            RebuildBrokerIndexes(_metadata);
        }

        // TODO: is called from ActionBlock but acess dictionaries. Unsafe!!!
        // TODO: unify with partition recovery flow
        async Task ResolveTopic(string topic)
        {
            var req = new TopicRequest { Topics = new[] { topic } };
            try 
            {
resend:
                var topicMeta = await _connection.MetadataRequest(req);
                switch (topicMeta.Topics[0].TopicErrorCode)
                {
                    case ErrorCode.NoError:
                        _scheduler.Schedule(async () => { 
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
                _scheduler.Schedule(() => {
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
            _scheduler.Schedule(() =>
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
            return await await _scheduler.Ask(() =>
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
            _scheduler.Schedule(() =>
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
                var brokers = await _scheduler.Ask(() => _metadata.Brokers.ToArray());

                foreach (var broker in brokers)
                {
                    // TODO: fail upon certain timeout
                    try
                    {
                        // any topic which has partitions in non-draining state. Draining can stay in the queue for some time
                        var topics = await _scheduler.Ask(() => _waitingMessages.Where(kv => kv.Value.Any(p => p.Key.ErrorCode != ErrorCode.Draining)).
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

                        var metaNew = await _connection.MetadataRequest(new TopicRequest { Topics = topics }, broker);
                        // if no exception, broker is back online. TODO: this is not thread-safe
                        if(broker.Conn.State == ConnState.Connecting)
                            broker.Conn.State = ConnState.Connected;

                        _scheduler.Schedule(() =>
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
            _scheduler.Schedule(async () => {
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
                    var response = await _connection.ProduceRaw(new ProduceRequest {Broker = broker, RequiredAcks = recovered.Pub.Acks, Timeout = recovered.Pub.TimeoutMs, TopicData = new[] {topicData}}, CancellationToken.None);

                    var errorCode = response.Topics.Single().Partitions.Single().ErrorCode;
                    if (errorCode == ErrorCode.NoError)
                    {
                        var recovered1 = recovered;  // just make compiler happy
                        _scheduler.Schedule(() => {
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

        enum BrokerState
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
            return _scheduler.Ask(() => _metadata.Topics.Select(t => t.TopicName).ToArray()).Result;
        }
    }
}
