using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using kafka4net;
using kafka4net.ConsumerImpl;
using kafka4net.Utils;
using NLog;
using NLog.Config;
using NLog.Targets;
using NUnit.Framework;
using Logger = kafka4net.Logger;

namespace tests
{
    [Target("Kafka4netEtwTarget")]
    public class Kafka4netEtwTarget : TargetWithLayout
    {
        protected override void Write(LogEventInfo logEvent)
        {
            kafka4net.Tracing.EtwTrace.Marker(logEvent.FormattedMessage);
        }
    }

    [TestFixture]
    class RecoveryTest
    {
        Random _rnd = new Random();
        string _seedAddresses = "192.168.56.10,192.168.56.20";
        //private const string _seedAddresses = "192.168.56.10";
        static readonly NLog.Logger _log = LogManager.GetCurrentClassLogger();

        [SetUp]
        public void Setup()
        {
            // NLog
            var config = new LoggingConfiguration();
            var consoleTarget = new ColoredConsoleTarget();
            config.AddTarget("console", consoleTarget);
            var fileTarget = new FileTarget();
            config.AddTarget("file", fileTarget);
            
            var etwTargt = new Kafka4netEtwTarget();
            config.AddTarget("etw", etwTargt);
            config.LoggingRules.Add(new LoggingRule("tests.*", LogLevel.Debug, etwTargt));

            consoleTarget.Layout = "${date:format=HH\\:mm\\:ss.fff} ${level} [${threadname}:${threadid}] ${logger} ${message} ${exception:format=tostring}";
            fileTarget.FileName = "${basedir}../../../../log.txt";
            fileTarget.Layout = "${longdate} ${level} [${threadname}:${threadid}] ${logger:shortName=true} ${message} ${exception:format=tostring,stacktrace:innerFormat=tostring,stacktrace}";

            var rule = new LoggingRule("*", LogLevel.Info, consoleTarget);
            rule.Targets.Add(fileTarget);
            //rule.Final = true;


            //var r1 = new LoggingRule("kafka4net.Internal.PartitionRecoveryMonitor", LogLevel.Info, fileTarget) { Final = true };
            //r1.Targets.Add(consoleTarget);
            //config.LoggingRules.Add(r1);

            rule = new LoggingRule("*", LogLevel.Debug, fileTarget);
            rule.ChildRules.Add(new LoggingRule("tests.*", LogLevel.Debug, consoleTarget));
            config.LoggingRules.Add(rule);

            // disable Transport noise
            // config.LoggingRules.Add(new LoggingRule("kafka4net.Protocol.Transport", LogLevel.Info, fileTarget) { Final = true});
            //
            //config.LoggingRules.Add(new LoggingRule("tests.*", LogLevel.Debug, fileTarget));
            //config.LoggingRules.Add(new LoggingRule("kafka4net.Internal.PartitionRecoveryMonitor", LogLevel.Error, fileTarget) { Final = true });
            //config.LoggingRules.Add(new LoggingRule("kafka4net.Connection", LogLevel.Error, fileTarget) { Final = true });
            //config.LoggingRules.Add(new LoggingRule("kafka4net.Protocols.Protocol", LogLevel.Error, fileTarget) { Final = true });
            //

            LogManager.Configuration = config;

            _log.Debug("=============== Starting =================");

            // set nlog logger in kafka
            Logger.SetupNLog();

            //
            // log4net
            //
            //var app1 = new log4net.Appender.FileAppender() { File = @"C:\projects\kafka4net\log2.txt" };
            //var coll = log4net.Config.BasicConfigurator.Configure();
            //var logger = log4net.LogManager.GetLogger("Main");
            //logger.Info("=============== Starting =================");
            //Logger.SetupLog4Net();

            TaskScheduler.UnobservedTaskException += (sender, args) => _log.Error("Unhandled task exception", (Exception)args.Exception);
            AppDomain.CurrentDomain.UnhandledException += (sender, args) => _log.Error("Unhandled exception", (Exception)args.ExceptionObject);

            // make sure brokers are up from last run
            //VagrantBrokerUtil.RestartBrokers();
        }

        [TearDown]
        public void RestartBrokers()
        {
            VagrantBrokerUtil.RestartBrokers();
        }

        [TestFixtureSetUp]
        public void BeforeEveryTest()
        {
            VagrantBrokerUtil.RestartBrokers();
        }

        // If failed to connect, messages are errored

        /// <summary>
        /// If topic does not exists, it is created when producer connects.
        /// Validate it by writing and than reading from newly created topic.
        /// </summary>
        [Test]
        public async void TopicIsAutocreatedByProducer()
        {
            kafka4net.Tracing.EtwTrace.Marker("TopicIsAutocreatedByProducer");

            var topic ="autocreate.test." + _rnd.Next();
            const int producedCount = 10;
            var lala = Encoding.UTF8.GetBytes("la-la-la");
            // TODO: set wait to 5sec

            //
            // Produce
            // In order to make sure that topic was created by producer, send and wait for producer
            // completion before performing validation read.
            //
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));

            await producer.ConnectAsync();

            _log.Debug("Producing...");
            await Observable.Interval(TimeSpan.FromSeconds(1)).
                Take(producedCount).
                Do(_ => producer.Send(new Message { Value = lala })).
                ToTask();
            await producer.CloseAsync(TimeSpan.FromSeconds(10));

            //
            // Validate by reading published messages
            //
            var receivedTxt = new List<string>();
            var complete = new BehaviorSubject<int>(0);
            Action<ReceivedMessage> handler = msg =>
            {
                var str = Encoding.UTF8.GetString(msg.Value);
                lock (receivedTxt)
                {
                    receivedTxt.Add(str);
                    complete.OnNext(receivedTxt.Count);
                }
            };
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPositionAtBeginning().
                WithAction(handler).
                Build();

            _log.Debug("Waiting for consumer");
            await complete.TakeWhile(i => i < producedCount).TakeUntil(DateTimeOffset.Now.AddSeconds(5)).LastOrDefaultAsync().ToTask();

            Assert.AreEqual(producedCount, receivedTxt.Count, "Did not receive all messages");
            Assert.IsTrue(receivedTxt.All(m => m == "la-la-la"), "Unexpected message content");

            consumer.Dispose();

            kafka4net.Tracing.EtwTrace.Marker("/TopicIsAutocreatedByProducer");
        }

        //[Test]
        //public void MultithreadingSend()
        //{
        //    // for 10 minutes, 15 threads, each to its own topic, 100ms interval, send messages.
        //    // In addition, 5 topics are produced by 
        //    // Consumer must validate that messages were sent in order and all were received
        //}

        //[Test]
        //public void Miltithreading2()
        //{
        //    // subjects are produced by 15, 7, 3, 3, 2, 2, 2, 2, producers/threads
        //    // Consumer must validate that messages were sent in order and all were received
        //}

        // consumer sees the same messages as producer

        /// <summary>
        /// If leader goes down, messages keep being accepted 
        /// and are committed (can be read) within (5sec?)
        /// Also, order of messages is preserved
        /// </summary>
        [Test]
        public async void LeaderDownProducerAndConsumerRecovery()
        {
            kafka4net.Tracing.EtwTrace.Marker("LeaderDownProducerAndConsumerRecovery");
            string topic = "part32." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 3, 2);

            var sent = new List<string>();
            var confirmedSent1 = new List<string>();

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            producer.OnSuccess += msgs =>
            {
                msgs.ForEach(msg => confirmedSent1.Add(Encoding.UTF8.GetString(msg.Value)));
                _log.Debug("Sent {0} messages", msgs.Length);
            };
            await producer.ConnectAsync();

            var received = new List<ReceivedMessage>();
            var receivedEvents = new ReplaySubject<ReceivedMessage>();
            Action<ReceivedMessage> consumerHandler = msg =>
            {
                lock(received)
                {
                    received.Add(msg);
                    receivedEvents.OnNext(msg);
                }
                _log.Debug("Received {0}/{1}", Encoding.UTF8.GetString(msg.Value), received.Count);
            };
            var consumer = Consumer.Create(_seedAddresses, topic).WithAction(consumerHandler).Build();

            const int postCount = 100;
            const int postCount2 = 50;

            //
            // Read messages
            //
            await consumer.State.Connected;

            //
            // Send #1
            //
            _log.Info("Start sender");
            Observable.Interval(TimeSpan.FromMilliseconds(200)).
                Take(postCount).
                Subscribe(
                    i => {
                        var msg = "msg " + i;
                        producer.Send(new Message { Value = Encoding.UTF8.GetBytes(msg) });
                        sent.Add("msg " + i);
                    },
                    () => _log.Info("Producer complete")
                );

            // wait for first 50 messages to arrive
            _log.Info("Waiting for first {0} messages to arrive", postCount2);
            await receivedEvents.Take(postCount2).Count().ToTask();
            Assert.AreEqual(postCount2, received.Count);

            _log.Info("Stopping broker");
            var stoppedBroker = VagrantBrokerUtil.StopBrokerLeaderForPartition(producer.Cluster, topic, 0);
            _log.Debug("Stopped broker {0}", stoppedBroker);

            // post another 50 messages
            _log.Info("Sending another {0} messages", postCount2);
            var sender2 = Observable.Interval(TimeSpan.FromMilliseconds(200)).
                Take(postCount2).
                Publish().RefCount();

            //
            // Send #2
            //
            sender2.Subscribe(
                    i => {
                        var msg = "msg #2 " + i;
                        producer.Send(new Message { Value = Encoding.UTF8.GetBytes(msg) });
                        sent.Add(msg);
                        _log.Debug("Sent msg #2 {0}", i);
                    },
                    () => _log.Info("Producer #2 complete")
                );

            _log.Info("Waiting for #2 sender to complete");
            await sender2.ToTask();
            _log.Info("Waiting for producer.Close");
            await producer.CloseAsync(TimeSpan.FromSeconds(60));

            _log.Info("Waiting 4sec for remaining messages");
            await Task.Delay(TimeSpan.FromSeconds(4)); // if unexpected messages arrive, let them in to detect failure

            _log.Info("Waiting for consumer.CloseAsync");
            consumer.Dispose();

            if (postCount + postCount2 != received.Count)
            {
                var receivedStr = received.Select(m => Encoding.UTF8.GetString(m.Value)).ToArray();

                var diff = sent.Except(received.Select(m => Encoding.UTF8.GetString(m.Value))).OrderBy(s => s);
                _log.Info("Not received {0}: \n {1}", diff.Count(), string.Join("\n ", diff));

                var diff2 = sent.Except(confirmedSent1).OrderBy(s => s);
                _log.Info("Not confirmed {0}: \n {1}", diff2.Count(), string.Join("\n ", diff2));

                var diff3 = received.Select(m => Encoding.UTF8.GetString(m.Value)).Except(sent).OrderBy(s => s);
                _log.Info("Received extra: {0}: \n {1}", diff3.Count(), string.Join("\n ", diff3));

                var diff4 = confirmedSent1.Except(sent).OrderBy(s => s);
                _log.Info("Confirmed extra {0}: \n {1}", diff4.Count(), string.Join("\n ", diff4));

                var dups = receivedStr.GroupBy(s => s).Where(g => g.Count() > 1).Select(g => string.Format("{0}: {1}", g.Count(), g.Key));
                _log.Info("Receved dups: \n {0}", string.Join("\n ", dups));

                _log.Debug("Received: \n{0}", string.Join("\n ", received.Select(m => Encoding.UTF8.GetString(m.Value))));
            }
            Assert.AreEqual(postCount + postCount2, received.Count, "Received.Count");

            _log.Info("Done");
            kafka4net.Tracing.EtwTrace.Marker("/LeaderDownProducerAndConsumerRecovery");
        }

        /// <summary>
        /// The check is implemented by querying offset information. It is expected to be 0.
        /// </summary>
        [Test]
        public async void ConsumerOnNonExistentTopicWaitsForTopicCreation()
        {
            kafka4net.Tracing.EtwTrace.Marker("ListenerOnNonExistentTopicWaitsForTopicCreation");
            const int numMessages = 400;
            var topic = "topic." + _rnd.Next();
            Action<ReceivedMessage> handler = msg =>
            {
                var i = BitConverter.ToInt32(msg.Value, 0);
                _log.Info("Received: {0}", i);
            };

            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPositionAtBeginning().
                WithAction(handler).
                Build();

            await consumer.State.Connected;
            var metas = await consumer.Cluster.GetOrFetchMetaForTopicAsync(topic);
            
            Assert.Greater(metas.Length, 0, "Expected at least one partition");
            Assert.IsTrue(metas.All(meta => meta.ErrorCode.IsSuccess()), "All partition statuses must be success");

            var offsets = await consumer.Cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);
            Assert.Greater(offsets.GetPartitionsOffset.Count, 0, "Expect at least some partitions offset");
            Assert.IsTrue(offsets.GetPartitionsOffset.Values.All(offset => offset == 0L), "Expected all offsets to be 0");

            consumer.Dispose();
            await consumer.State.Closed;
            
            kafka4net.Tracing.EtwTrace.Marker("/ListenerOnNonExistentTopicWaitsForTopicCreation");
        }


        /// <summary>
        /// Test listener and producer recovery together
        /// </summary>
        [Test]
        public async void ProducerAndListenerRecoveryTest()
        {
            kafka4net.Tracing.EtwTrace.Marker("ProducerAndListenerRecoveryTest");
            const int count = 200;
            var topic = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,6,3);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));

            _log.Debug("Connecting");
            await producer.ConnectAsync();

            _log.Debug("Filling out {0}", topic);
            var sentList = new List<int>(200);
            Observable.Interval(TimeSpan.FromMilliseconds(100))
                .Select(l => (int) l)
                .Select(i => new Message {Value = BitConverter.GetBytes(i)})
                .Take(count)
                .Subscribe(msg=> { producer.Send(msg); sentList.Add(BitConverter.ToInt32(msg.Value, 0)); });

            var current = 0;
            var received = new ReplaySubject<ReceivedMessage>();
            Task brokerStopped = null;
            Consumer consumer = null;
            Action<ReceivedMessage> handler = msg =>
            {
                current++;
                if (current == 18)
                {
                    brokerStopped = Task.Factory.StartNew(() => VagrantBrokerUtil.StopBrokerLeaderForPartition(consumer.Cluster, consumer.Topic, msg.Partition), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                    _log.Info("Stopped Broker Leader {0}", brokerStopped);
                }
                received.OnNext(msg);
                _log.Info("Got: {0}", BitConverter.ToInt32(msg.Value, 0));
            };

            consumer = Consumer.Create(_seedAddresses, topic).
                WithAction(handler).
                WithStartPositionAtBeginning().
                WithMaxBytesPerFetch(4 * 8).
                Build();

            await consumer.State.Connected;

            _log.Info("Waiting for receiver complete");
            var receivedList = await received.Select(msg => BitConverter.ToInt32(msg.Value, 0)).Take(count).TakeUntil(DateTime.Now.AddSeconds(60)).ToList().ToTask();

            await brokerStopped.TimeoutAfter(TimeSpan.FromSeconds(10));

            // get the offsets for comparison later
            var heads = await consumer.Cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);
            var tails = await consumer.Cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);

            _log.Info("Done waiting for receiver. Closing producer.");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed, disposing consumer subscription.");
            _log.Info("Consumer subscription disposed. Closing consumer.");
            consumer.Dispose();
            await consumer.State.Closed;
            _log.Info("Consumer closed.");

            if (sentList.Count != receivedList.Count)
            {
                // log some debug info.
                _log.Error("Did not receive all messages. Messages received: {0}",string.Join(",",receivedList.OrderBy(i=>i)));
                _log.Error("Did not receive all messages. Messages sent but NOT received: {0}", string.Join(",", sentList.Except(receivedList).OrderBy(i => i)));

                _log.Error("Sum of offsets fetched: {0}", tails.MessagesSince(heads));
                _log.Error("Offsets fetched: [{0}]", string.Join(",", tails.Partitions.Select(p => string.Format("{0}:{1}",p,tails.NextOffset(p)))));
            }

            Assert.AreEqual(sentList.Count, receivedList.Count);
            kafka4net.Tracing.EtwTrace.Marker("/ProducerAndListenerRecoveryTest");
        }

        /// <summary>
        /// Test producer recovery isolated.
        /// 
        /// Create queue and produce 200 messages. On message 30, shutdown broker#2
        /// Check that result summ of offsets (legth of the topic) is equal to 200
        /// Check confirmed sent messasges count is sequal to intendent sent.
        /// </summary>
        [Test]
        public async void ProducerRecoveryTest()
        {
            kafka4net.Tracing.EtwTrace.Marker("ProducerRecoveryTest");

            const int count = 200;
            var topic = "part62." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 6, 2);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));

            _log.Debug("Connecting");
            await producer.ConnectAsync();

            _log.Debug("Filling out {0}", topic);
            // when we get a confirm back, add to list actually sent.
            var actuallySentList = new List<int>(count);
            producer.OnSuccess += msgs => actuallySentList.AddRange(msgs.Select(msg => BitConverter.ToInt32(msg.Value, 0)));

            Task stopBrokerTask = null;
            var sentList = await Observable.Interval(TimeSpan.FromMilliseconds(100))
                .Select(l => (int)l)
                .Do(l => { if (l == 20) stopBrokerTask = Task.Factory.StartNew(() => VagrantBrokerUtil.StopBroker("broker2"), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default); })
                .Select(i => new Message { Value = BitConverter.GetBytes(i) })
                .Take(count)
                .Do(producer.Send)
                .Select(msg => BitConverter.ToInt32(msg.Value, 0))
                .ToList();


            _log.Info("Done waiting for sending. Closing producer.");
            await producer.CloseAsync(TimeSpan.FromSeconds(30));
            _log.Info("Producer closed.");

            if (stopBrokerTask != null)
                await stopBrokerTask.TimeoutAfter(TimeSpan.FromSeconds(10));

            //
            // Check length of result topic
            //
            var c2 = new Cluster(_seedAddresses);
            await c2.ConnectAsync();
            var heads = await c2.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);
            var tails = await c2.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);

            _log.Info("Sum of offsets: {0}", tails.MessagesSince(heads));
            _log.Info("Offsets: [{0}]", string.Join(",", tails.Partitions.Select(p => string.Format("{0}:{1}", p, tails.NextOffset(p)))));

            //
            if (sentList.Count != actuallySentList.Count)
            {
                // log some debug info.
                _log.Error("Did not send all messages. Messages sent but NOT acknowledged: {0}", string.Join(",", sentList.Except(actuallySentList).OrderBy(i => i)));
            }

            Assert.AreEqual(sentList.Count, actuallySentList.Count, "Actually sent");
            Assert.AreEqual(sentList.Count, tails.MessagesSince(heads), "Offsets");
            
            kafka4net.Tracing.EtwTrace.Marker("/ProducerRecoveryTest");
        }

        /// <summary>
        /// Test just listener recovery isolated from producer
        /// </summary>
        [Test]
        public async void ListenerRecoveryTest()
        {
            kafka4net.Tracing.EtwTrace.Marker("ListenerRecoveryTest");
            const int count = 10000;
            var topic = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 6, 3);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            _log.Debug("Connecting");
            await producer.ConnectAsync();

            _log.Debug("Filling out {0} with {1} messages", topic, count);
            var sentList = await Enumerable.Range(0, count)
                .Select(i => new Message { Value = BitConverter.GetBytes(i) })
                .ToObservable()
                .Do(producer.Send)
                .Select(msg => BitConverter.ToInt32(msg.Value, 0))
                .ToList();

            await Task.Delay(TimeSpan.FromSeconds(1));

            var heads = await producer.Cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);
            var tails = await producer.Cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);

            _log.Info("Done sending messages. Closing producer.");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed, starting consumer subscription.");

            var messagesInTopic = (int)tails.MessagesSince(heads);
            _log.Info("Topic offsets indicate producer sent {0} messages.", messagesInTopic);

            var current = 0;
            var received = new ReplaySubject<ReceivedMessage>();
            Task stopBrokerTask = null;
            Consumer consumer = null;
            Action<ReceivedMessage> handler = msg =>
            {
                current++;
                if (current == 18)
                {
                    stopBrokerTask = Task.Factory.StartNew(() => VagrantBrokerUtil.StopBrokerLeaderForPartition(consumer.Cluster, consumer.Topic, msg.Partition), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                }
                received.OnNext(msg);
            };

            consumer = Consumer.Create(_seedAddresses, topic).
                WithAction(handler).
                WithStartPositionAtBeginning().
                WithMaxBytesPerFetch(4 * 8).
                Build();

            await consumer.State.Connected;

            _log.Info("Waiting for receiver complete");
            var receivedList = await received.Select(msg => BitConverter.ToInt32(msg.Value, 0)).Take(messagesInTopic).
                TakeUntil(DateTime.Now.AddSeconds(60)).ToList().ToTask();

            if (stopBrokerTask != null)
                await stopBrokerTask.TimeoutAfter(TimeSpan.FromSeconds(10));

            tails = await consumer.Cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);

            _log.Info("Consumer subscription disposed. Closing consumer.");
            consumer.Dispose();
            await consumer.State.Closed;
            _log.Info("Consumer closed.");

            _log.Info("Sum of offsets: {0}", tails.MessagesSince(heads));
            _log.Info("Offsets: [{0}]", string.Join(",", tails.Partitions.Select(p => string.Format("{0}:{1}", p, tails.NextOffset(p)))));

            if (messagesInTopic != receivedList.Count)
            {
                // log some debug info.
                _log.Error("Did not receive all messages. Messages sent but NOT received: {0}", string.Join(",", sentList.Except(receivedList).OrderBy(i => i)));

            }

            Assert.AreEqual(messagesInTopic, receivedList.Count);
            kafka4net.Tracing.EtwTrace.Marker("/ListenerRecoveryTest");
        }

        /// <summary>
        /// Set long batching period for producer (20 sec) and make sure that shutdown flushes
        /// buffered data.
        /// </summary>
        [Test]
        public async void CleanShutdownTest()
        {
            kafka4net.Tracing.EtwTrace.Marker("CleanShutdownTest");
            const string topic = "shutdown.test";

            // set producer long batching period, 20 sec
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic, TimeSpan.FromSeconds(20), int.MaxValue));

            _log.Debug("Connecting");
            await producer.ConnectAsync();

            var received = new HashSet<string>();
            Action<ReceivedMessage> handler = msg => {
                var str = Encoding.UTF8.GetString(msg.Value);
                received.Add(str);
            };

            // start listener at the end of queue and accumulate received messages
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithAction(handler).
                WithMaxWaitTimeMs(30 * 1000).
                Build();

            _log.Info("Connecting consumer");
            await consumer.State.Connected;
            _log.Info("Subscribed to consumer");

            _log.Info("Starting sender");
            // send data, 5 msg/sec, for 5 seconds
            var sent = new HashSet<string>();
            var sender = Observable.Interval(TimeSpan.FromSeconds(1.0 / 5)).
                Select(i => string.Format("msg {0} {1}", i, Guid.NewGuid())).
                Synchronize().
                Do(m => sent.Add(m)).
                Select(msg => new Message
                {
                    Value = Encoding.UTF8.GetBytes(msg)
                }).
                TakeUntil(DateTimeOffset.Now.AddSeconds(5)).
                Publish().RefCount();
            sender.Subscribe(producer.Send);

            _log.Debug("Waiting for sender");
            await sender;
            _log.Debug("Waiting for producer complete");
            await producer.CloseAsync(TimeSpan.FromSeconds(4));

            // how to make sure nothing is sent after shutdown? listen to logger?  have connection events?

            // wait for 5sec for receiver to get all the messages
            _log.Info("Waiting for consumer to fetch");
            await Task.Delay(5000);
            _log.Info("Closing consumer");
            consumer.Dispose();
            await consumer.State.Closed;
            _log.Info("Closed consumer");

            // assert we received all the messages

            Assert.AreEqual(sent.Count, received.Count, string.Format("Sent and Receved size differs. Sent: {0} Recevied: {1}", sent.Count, received.Count));
            // compare sets and not lists, because of 2 partitions, send order and receive orser are not the same
            Assert.True(received.SetEquals(sent), "Sent and Received set differs");

            kafka4net.Tracing.EtwTrace.Marker("/CleanShutdownTest");
        }

        //[Test]
        //public void DirtyShutdownTest()
        //{

        //}

        /// <summary>
        /// Shut down 2 brokers which will forse broker1 to become a leader.
        /// Publish 25K messages.
        /// Calculate sent message count as difference between heads and tails
        /// Start consumer and on 18th message start rebalance.
        /// Wait for consuming all messages but with 120sec timeout.
        /// 
        /// </summary>
        [Ignore]
        public async void ConsumerFollowsRebalancingPartitions()
        {
            kafka4net.Tracing.EtwTrace.Marker("ConsumerFollowsRebalancingPartitions");

            // create a topic
            var topic = "topic33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,11,3);

            // Stop two brokers to let leadership shift to broker1.
            VagrantBrokerUtil.StopBroker("broker2");
            VagrantBrokerUtil.StopBroker("broker3");

            await Task.Delay(TimeSpan.FromSeconds(5));

            // now start back up
            VagrantBrokerUtil.StartBroker("broker2");
            VagrantBrokerUtil.StartBroker("broker3");

            // wait a little for everything to start
            await Task.Delay(TimeSpan.FromSeconds(5));

            // we should have all of them with leader 1
            var cluster = new Cluster(_seedAddresses);
            await cluster.ConnectAsync();
            var partitionMeta = await cluster.GetOrFetchMetaForTopicAsync(topic);

            // make sure they're all on a single leader
            Assert.AreEqual(1, partitionMeta.GroupBy(p=>p.Leader).Count());

            // now publish messages
            const int count = 25000;
            var producer = new Producer(cluster, new ProducerConfiguration(topic));
            _log.Debug("Connecting");
            await producer.ConnectAsync();

            _log.Debug("Filling out {0} with {1} messages", topic, count);
            var sentList = await Enumerable.Range(0, count)
                .Select(i => new Message { Value = BitConverter.GetBytes(i) })
                .ToObservable()
                .Do(producer.Send)
                .Select(msg => BitConverter.ToInt32(msg.Value, 0))
                .ToList();

            await Task.Delay(TimeSpan.FromSeconds(1));

            _log.Info("Done sending messages. Closing producer.");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed, starting consumer subscription.");

            await Task.Delay(TimeSpan.FromSeconds(1));
            var heads = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);
            var tails = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);

            var messagesInTopic = (int)tails.MessagesSince(heads);
            _log.Info("Topic offsets indicate producer sent {0} messages.", messagesInTopic);

            var current = 0;
            var received = new ReplaySubject<ReceivedMessage>();
            Task rebalanceTask = null;
            Action<ReceivedMessage> handler = msg =>
            {
                current++;
                if (current == 18)
                {
                    rebalanceTask = Task.Factory.StartNew(VagrantBrokerUtil.RebalanceLeadership, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                }
                received.OnNext(msg);
            };

            //var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, new StartPositionTopicStart(), maxBytesPerFetch: 4 * 8));
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithAction(handler).
                WithMaxBytesPerFetch(4 * 8).
                Build();
            await consumer.State.Connected;

            _log.Info("Waiting for receiver complete");
            var receivedList = await received.Select(msg => BitConverter.ToInt32(msg.Value, 0)).
                Take(messagesInTopic).
                TakeUntil(DateTime.Now.AddMinutes(3)).
                ToList().
                ToTask();
            if (rebalanceTask != null)
            {
                _log.Info("Waiting for rebalance complete");
                await rebalanceTask;//.TimeoutAfter(TimeSpan.FromSeconds(10));
                _log.Info("Rebalance complete");
            }

            _log.Info("Consumer subscription disposed. Closing consumer.");
            consumer.Dispose();
            await consumer.State.Closed;
            _log.Info("Consumer closed.");

            tails = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);

            await cluster.CloseAsync(TimeSpan.FromSeconds(5));

            _log.Info("Sum of offsets: {0}", tails.MessagesSince(heads));
            _log.Info("Offsets: [{0}]", string.Join(",", tails.Partitions.Select(p => string.Format("{0}:{1}", p, tails.NextOffset(p)))));

            if (messagesInTopic != receivedList.Count)
            {
                // log some debug info.
                _log.Error("Did not receive all messages. Messages sent but NOT received: {0}", string.Join(",", sentList.Except(receivedList).OrderBy(i => i)));

            }

            Assert.AreEqual(messagesInTopic, receivedList.Count);


            kafka4net.Tracing.EtwTrace.Marker("/ConsumerFollowsRebalancingPartitions");
        }

        /// <summary>
        /// Send and listen for 100sec. Than wait for receiver to catch up for 10sec. 
        /// Compare sent and received message count.
        /// Make sure that messages were received in the same order as they were sent.
        /// </summary>
        [Test]
        public async void KeyedMessagesPreserveOrder()
        {
            kafka4net.Tracing.EtwTrace.Marker("KeyedMessagesPreserveOrder");
            // create a topic with 3 partitions
            var topicName = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topicName, 3, 3);

            var receivedMsgs = new List<ReceivedMessage>();
            Action<ReceivedMessage> handler = msg =>
            {
                lock (receivedMsgs)
                {
                    receivedMsgs.Add(msg);
                }
            };

            // create listener in a separate connection/broker
            var consumer = Consumer.Create(_seedAddresses, topicName).
                WithAction(handler).
                Build();
            await consumer.State.Connected;

            // sender is configured with 50ms batch period
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topicName, TimeSpan.FromMilliseconds(50)));
            await producer.ConnectAsync();

            //
            // generate messages with 100ms interval in 10 threads
            //
            var sentMsgs = new List<Message>();
            _log.Info("Start sending");
            var senders = Enumerable.Range(1, 1).
                Select(thread => Observable.
                    Interval(TimeSpan.FromMilliseconds(10)).
                    Synchronize(). // protect adding to sentMsgs
                    Select(i =>
                    {
                        var str = "msg " + i + " thread " + thread + " " + Guid.NewGuid();
                        var bin = Encoding.UTF8.GetBytes(str);
                        var msg = new Message
                        {
                            Key = BitConverter.GetBytes((int)(i + thread) % 10),
                            Value = bin
                        };
                        return Tuple.Create(msg, i, str);
                    }).
                    Subscribe(msg =>
                    {
                        lock (sentMsgs)
                        {
                            producer.Send(msg.Item1);
                            sentMsgs.Add(msg.Item1);
                            Assert.AreEqual(msg.Item2, sentMsgs.Count-1);
                        }
                    })
                ).
                ToArray();

            // wait for around 10K messages (10K/(10*10) = 100sec) and close producer
            _log.Info("Waiting for producer to produce enough...");
            await Task.Delay(100*1000);
            _log.Info("Closing senders intervals");
            senders.ForEach(s => s.Dispose());
            _log.Info("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            _log.Info("Waiting for additional 10sec");
            await Task.Delay(10*1000);

            _log.Info("Closing consumer");
            consumer.Dispose();
            await consumer.State.Closed;
            _log.Info("Done with networking");

            // compare sent and received messages
            // TODO: for some reason preformance is not what I'd expect it to be and only 6K is generated.
            Assert.GreaterOrEqual(sentMsgs.Count, 4000, "Expected around 10K messages to be sent");

            if (sentMsgs.Count != receivedMsgs.Count)
            {
                var sentStr = sentMsgs.Select(m => Encoding.UTF8.GetString(m.Value)).ToArray();
                var receivedStr = receivedMsgs.Select(m => Encoding.UTF8.GetString(m.Value)).ToArray();
                sentStr.Except(receivedStr).
                    ForEach(m => _log.Error("Not received: '{0}'", m));
                receivedStr.Except(sentStr).
                    ForEach(m => _log.Error("Not sent but received: '{0}'", m));
            }
            Assert.AreEqual(sentMsgs.Count, receivedMsgs.Count, "Sent and received messages count differs");
            
            //
            // group messages by key and compare lists in each key to be the same (order should be preserved within key)
            //
            var keysSent = sentMsgs.GroupBy(m => BitConverter.ToInt32(m.Key, 0), m => Encoding.UTF8.GetString(m.Value), (i, mm) => new { Key = i, Msgs = mm.ToArray() }).ToArray();
            var keysReceived = receivedMsgs.GroupBy(m => BitConverter.ToInt32(m.Key, 0), m => Encoding.UTF8.GetString(m.Value), (i, mm) => new { Key = i, Msgs = mm.ToArray() }).ToArray();
            Assert.AreEqual(10, keysSent.Count(), "Expected 10 unique keys 0-9");
            Assert.AreEqual(keysSent.Count(), keysReceived.Count(), "Keys count does not match");
            // compare order within each key
            var notInOrder = keysSent
                .OrderBy(k => k.Key)
                .Zip(keysReceived.OrderBy(k => k.Key), (s, r) => new { s, r, ok = s.Msgs.SequenceEqual(r.Msgs) }).Where(_ => !_.ok).ToArray();

            if (notInOrder.Any())
            {
                _log.Error("{0} keys are out of order", notInOrder.Count());
                notInOrder.ForEach(_ => _log.Error("Failed order in:\n{0}", 
                    string.Join(" \n", DumpOutOfOrder(_.s.Msgs, _.r.Msgs))));
            }
            Assert.IsTrue(!notInOrder.Any(), "Detected out of order messages");

            kafka4net.Tracing.EtwTrace.Marker("/KeyedMessagesPreserveOrder");
        }

        // take 100 items starting from the ones which differ
        static IEnumerable<Tuple<string, string>> DumpOutOfOrder(IEnumerable<string> l1, IEnumerable<string> l2)
        {
            return l1.Zip(l2, (l, r) => new {l, r}).
                SkipWhile(_ => _.l == _.r).Take(100).
                Select(_ => Tuple.Create(_.l, _.r));
        }

        [Test]
        public async void ProducerSendBufferGrowsAutomatically()
        {
            kafka4net.Tracing.EtwTrace.Marker("ProducerSendBufferGrowsAutomatically");

            // now publish messages
            const int count2 = 25000;
            var topic = "part13." + _rnd.Next();
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            _log.Debug("Connecting");
            await producer.ConnectAsync();

            _log.Debug("Filling out {0} with {1} messages", topic, count2);
            var sentList = await Enumerable.Range(0, count2)
                .Select(i => new Message { Value = BitConverter.GetBytes(i) })
                .ToObservable()
                .Do(producer.Send)
                .Select(msg => BitConverter.ToInt32(msg.Value, 0))
                .ToList();

            await Task.Delay(TimeSpan.FromSeconds(1));

            _log.Info("Done sending messages. Closing producer.");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed, starting consumer subscription.");

            
            
            // create a topic with 3 partitions
            var topicName = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topicName, 3, 3);

            // sender is configured with 50ms batch period
            var receivedSubject = new ReplaySubject<Message>();
            producer = new Producer(_seedAddresses,
                new ProducerConfiguration(topicName, TimeSpan.FromMilliseconds(50), sendBuffersInitialSize: 1));
            producer.OnSuccess += ms => ms.ForEach(receivedSubject.OnNext);
            await producer.ConnectAsync();

            // send 1000 messages
            const int count = 1000;
            await Observable.Interval(TimeSpan.FromMilliseconds(10))
                .Do(l => producer.Send(new Message() {Value = BitConverter.GetBytes((int) l)}))
                .Take(count);

            var receivedMessages = await receivedSubject.Take(count).TakeUntil(DateTime.Now.AddSeconds(2)).ToArray();

            Assert.AreEqual(count,receivedMessages.Length);

            await producer.CloseAsync(TimeSpan.FromSeconds(5));
            
            kafka4net.Tracing.EtwTrace.Marker("/ProducerSendBufferGrowsAutomatically");
        }

        // explicit offset works
        [Test]
        public async void ExplicitOffset()
        {
            kafka4net.Tracing.EtwTrace.Marker("ExplicitOffset");
            // create new topic with 3 partitions
            var topic = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,3,3);

            // fill it out with 10K messages
            const int count = 10*1000;
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            await producer.ConnectAsync();

            var sentMessagesObservable = Observable.FromEvent<Message[]>(evtHandler => producer.OnSuccess += evtHandler, evtHandler => { })
                .SelectMany(msgs=>msgs)
                .Take(count)
                .TakeUntil(DateTime.Now.AddSeconds(10))
                .ToList();

            _log.Info("Sending data");
            Enumerable.Range(1, count).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            var sentMsgs = await sentMessagesObservable;
            _log.Info("Producer sent {0} messages.", sentMsgs.Count);

            _log.Debug("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            var offsetFetchCluster = new Cluster(_seedAddresses);
            await offsetFetchCluster.ConnectAsync();

            // consume tail-300 for each partition
            await Task.Delay(TimeSpan.FromSeconds(1));
            var offsets = new TopicPartitionOffsets(
                                topic, (await offsetFetchCluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd))
                                        .GetPartitionsOffset.Select(kv=>new KeyValuePair<int,long>(kv.Key,kv.Value-300)));
            _log.Info("Sum of offsets {0}. Raw: {1}",offsets.Partitions.Sum(p=>offsets.NextOffset(p)), offsets);

            var messagesTarget = new BufferBlock<ReceivedMessage>();

            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPosition(offsets).
                WithAction(messagesTarget).
                Build();
            
            var messages = messagesTarget.AsObservable().
                GroupBy(m => m.Partition).Replay();
            messages.Connect();
            await consumer.State.Connected;

            var consumerSubscription = messages.Subscribe(p => p.Take(10).Subscribe(
                m => _log.Debug("Got message {0}/{1}", m.Partition, BitConverter.ToInt32(m.Value, 0)),
                e => _log.Error("Error", e),
                () => _log.Debug("Complete part {0}", p.Key)
            ));

            // wait for 3 partitions to arrrive and every partition to read at least 100 messages
            await messages.Select(g => g.Take(100)).Take(3).ToTask();

            consumerSubscription.Dispose();
            consumer.Dispose();

            kafka4net.Tracing.EtwTrace.Marker("/ExplicitOffset");
        }

        [Test]
        public async void StopAtExplicitOffset()
        {
            kafka4net.Tracing.EtwTrace.Marker("StopAtExplicitOffset");
            // create new topic with 3 partitions
            var topic = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 3, 3);

            // fill it out with 10K messages
            const int count = 10 * 1000;
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            await producer.ConnectAsync();

            var sentMessagesObservable = Observable.FromEvent<Message[]>(evtHandler => producer.OnSuccess += evtHandler, evtHandler => { })
                .SelectMany(msgs => msgs)
                .Take(count)
                .TakeUntil(DateTime.Now.AddSeconds(10))
                .ToList();

            _log.Info("Sending data");
            Enumerable.Range(1, count).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            var sentMsgs = await sentMessagesObservable;
            _log.Info("Producer sent {0} messages.", sentMsgs.Count);

            _log.Debug("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            var offsetFetchCluster = new Cluster(_seedAddresses);
            await offsetFetchCluster.ConnectAsync();

            await Task.Delay(TimeSpan.FromSeconds(1));
            var offsets = (await offsetFetchCluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd));
            _log.Info("Sum of offsets {0}. Raw: {1}", offsets.Partitions.Sum(p => offsets.NextOffset(p)), offsets);

            // consume first 300 for each partition
            var offsetStops = new TopicPartitionOffsets(topic, offsets.GetPartitionsOffset.ToDictionary(pair => pair.Key, pair => pair.Value > 300 ? 300 : pair.Value));
            var numMessages = offsetStops.Partitions.Sum(p => offsetStops.NextOffset(p));
            _log.Info("Attempting to consume {0} messages and stop at {1}", numMessages, offsetStops);

            var messagesTarget = new BufferBlock<ReceivedMessage>();
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithAction(messagesTarget).
                WithStartPositionAtBeginning().
                WithStopPosition(offsetStops).
                Build();
            var messages = await messagesTarget.AsObservable().ToList();
            
            consumer.Dispose();

            Assert.AreEqual(numMessages, messages.Count);

            kafka4net.Tracing.EtwTrace.Marker("/StopAtExplicitOffset");
        }

        [Test]
        public async void StartAndStopAtExplicitOffset()
        {
            kafka4net.Tracing.EtwTrace.Marker("StartAndStopAtExplicitOffset");
            // create new topic with 3 partitions
            var topic = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 3, 3);

            // fill it out with 10K messages
            const int count = 10 * 1000;
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            await producer.ConnectAsync();

            var sentMessagesObservable = Observable.FromEvent<Message[]>(evtHandler => producer.OnSuccess += evtHandler, evtHandler => { })
                .SelectMany(msgs => msgs)
                .Take(count)
                .TakeUntil(DateTime.Now.AddSeconds(10))
                .ToList();

            _log.Info("Sending data");
            Enumerable.Range(1, count).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            var sentMsgs = await sentMessagesObservable;
            _log.Info("Producer sent {0} messages.", sentMsgs.Count);

            _log.Debug("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            var offsetFetchCluster = new Cluster(_seedAddresses);
            await offsetFetchCluster.ConnectAsync();

            await Task.Delay(TimeSpan.FromSeconds(1));
            var offsets = (await offsetFetchCluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd));
            _log.Info("Sum of offsets {0}. Raw: {1}", offsets.Partitions.Sum(p => offsets.NextOffset(p)), offsets);

            // consume first 300 for each partition
            var offsetStarts = new TopicPartitionOffsets(topic, offsets.GetPartitionsOffset.ToDictionary(pair => pair.Key, pair => pair.Value > 300 ? 300 : pair.Value));
            var offsetStops = new TopicPartitionOffsets(topic, offsets.GetPartitionsOffset.ToDictionary(pair => pair.Key, pair => pair.Value > 600 ? 600 : pair.Value));
            var numMessages = offsetStops.MessagesSince(offsetStarts);
            var startStopProvider = new StartAndStopAtExplicitOffsets(offsetStarts, offsetStops);
            _log.Info("Attempting to consume {0} messages and stop at {1}", numMessages, offsetStops);

            var targetMessages = new BufferBlock<ReceivedMessage>();

            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPosition(startStopProvider).
                WithStopPosition(startStopProvider).
                WithAction(targetMessages).
                Build();
            var messages = await targetMessages.AsObservable().ToList();

            consumer.Dispose();

            Assert.AreEqual(numMessages, messages.Count);

            kafka4net.Tracing.EtwTrace.Marker("/StartAndStopAtExplicitOffset");
        }

        [Test]
        public async void StopAtExplicitOffsetOnEmptyTopic()
        {
            kafka4net.Tracing.EtwTrace.Marker("StopAtExplicitOffsetOnEmptyTopic");
            // create new topic with 3 partitions
            var topic = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 3, 3);

            var offsetFetchCluster = new Cluster(_seedAddresses);
            await offsetFetchCluster.ConnectAsync();

            await Task.Delay(TimeSpan.FromSeconds(1));
            var offsets = (await offsetFetchCluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd));
            _log.Info("Sum of offsets {0}. Raw: {1}", offsets.Partitions.Sum(p => offsets.NextOffset(p)), offsets);

            var startStopProvider = new StartAndStopAtExplicitOffsets(offsets, offsets);

            _log.Info("Attempting to consume {0} messages and stop at {1}", 0, offsets);

            var targetMessages = new BufferBlock<ReceivedMessage>();

            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPosition(startStopProvider).
                WithStopPosition(startStopProvider).
                WithAction(targetMessages).
                Build();
            var startTime = DateTime.Now;
            var timeout = startTime.AddSeconds(30);
            var messages = await targetMessages.AsObservable().TakeUntil(timeout).ToList();
            _log.Info("Finished");
            Assert.IsTrue(DateTime.Now < timeout);
            Assert.AreEqual(0, messages.Count);
            consumer.Dispose();

            kafka4net.Tracing.EtwTrace.Marker("/StopAtExplicitOffsetOnEmptyTopic");
        }

        // can read from the head of queue
        [Test]
        public async void ReadFromHead()
        {
            kafka4net.Tracing.EtwTrace.Marker("ReadFromHead");

            const int count = 100;
            var topic = "part32." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,3,2);

            // fill it out with 100 messages
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            await producer.ConnectAsync();

            _log.Info("Sending data");
            Enumerable.Range(1, count).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            _log.Debug("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            // read starting from the head
            var targetMessages = new BufferBlock<ReceivedMessage>();
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPositionAtBeginning().
                WithAction(targetMessages).
                Build();
            var count2 = await targetMessages.AsObservable().TakeUntil(DateTimeOffset.Now.AddSeconds(5))
                //.Do(val=>_log.Info("received value {0}", BitConverter.ToInt32(val.Value,0)))
                .Count().ToTask();
            //await consumer.IsConnected;
            Assert.AreEqual(count, count2);

            kafka4net.Tracing.EtwTrace.Marker("/ReadFromHead");
        }

        // if attempt to fetch from offset out of range, excption is thrown
        //[Test]
        //public void OutOfRangeOffsetThrows()
        //{

        //}

        //// implicit offset is defaulted to fetching from the end
        //[Test]
        //public void DefaultPositionToTheTail()
        //{

        //}

        [Test]
        public void TopicPartitionOffsetsSerializeAndDeSerialize()
        {
            kafka4net.Tracing.EtwTrace.Marker("TopicPartitionOffsetsSerializeAndDeSerialize");

            var offsets1 = new TopicPartitionOffsets("test");

            for (int i = 0; i < 50; i++)
            {
                offsets1.UpdateOffset(i,_rnd.Next());
            }

            // save bytes
            var offsetBytes = offsets1.WriteOffsets();

            var offsets2 = new TopicPartitionOffsets(offsetBytes);

            for (int i = 0; i < 50; i++)
            {
                Assert.AreEqual(offsets1.NextOffset(i),offsets2.NextOffset(i));
            }

            kafka4net.Tracing.EtwTrace.Marker("/TopicPartitionOffsetsSerializeAndDeSerialize");
        }

        [Test]
        public async void SaveOffsetsAndResumeConsuming()
        {
            kafka4net.Tracing.EtwTrace.Marker("SaveOffsetsAndResumeConsuming");

            var sentEvents = new Subject<Message>();
            var topic = "part12." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 5, 2);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            producer.OnSuccess += e => e.ForEach(sentEvents.OnNext);
            await producer.ConnectAsync();

            // send 100 messages
            Enumerable.Range(1, 100).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);
            _log.Info("Waiting for 100 sent messages");
            sentEvents.Subscribe(msg => _log.Debug("Sent {0}", BitConverter.ToInt32(msg.Value, 0)));
            await sentEvents.Take(100).ToTask();


            var offsets1 = await producer.Cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);

            _log.Info("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            var handler1 = new BufferBlock<ReceivedMessage>();

            // now consume the "first" 50. Stop, save offsets, and restart.
            var consumer1 = Consumer.Create(_seedAddresses, topic).
                WithStartPosition(offsets1).WithAction(handler1).
                Build();
            var receivedEvents = new List<int>(100);

            _log.Info("Consuming first half of messages.");

            await handler1.AsObservable()
                .Do(msg =>
                {
                    var value = BitConverter.ToInt32(msg.Value, 0);
                    _log.Info("Consumer1 Received value {0} from partition {1} at offset {2}", value, msg.Partition, msg.Offset);
                    receivedEvents.Add(value);
                    offsets1.UpdateOffset(msg.Partition, msg.Offset);
                })
                .Take(50);
            //await consumer1.IsConnected;

            _log.Info("Closing first consumer");
            consumer1.Dispose();

            // now serialize the offsets.
            var offsetBytes = offsets1.WriteOffsets();

            // load a new set of offsets, and a new consumer
            var offsets2 = new TopicPartitionOffsets(offsetBytes);

            var target2 = new BufferBlock<ReceivedMessage>();
            var consumer2 = Consumer.Create(_seedAddresses, topic).
                WithStartPosition(offsets2).
                WithAction(target2).
                Build();

            await target2.AsObservable()
                .Do(msg =>
                {
                    var value = BitConverter.ToInt32(msg.Value, 0);
                    _log.Info("Consumer2 Received value {0} from partition {1} at offset {2}", value, msg.Partition, msg.Offset);
                    receivedEvents.Add(value);
                    offsets2.UpdateOffset(msg.Partition, msg.Offset);
                })
                .Take(50);
            //await consumer2.IsConnected;

            _log.Info("Closing second consumer");
            consumer2.Dispose();

            Assert.AreEqual(100, receivedEvents.Distinct().Count());
            Assert.AreEqual(100, receivedEvents.Count);

            kafka4net.Tracing.EtwTrace.Marker("/SaveOffsetsAndResumeConsuming");
        }

        // Create a new 1-partition topic and sent 100 messages.
        // Read offsets, they should be [0, 100]
        [Test]
        public async void ReadOffsets()
        {
            kafka4net.Tracing.EtwTrace.Marker("ReadOffsets");

            var sentEvents = new Subject<Message>();
            var topic = "part12." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,1,2);

            var cluster = new Cluster(_seedAddresses);
            await cluster.ConnectAsync();
            var producer = new Producer(cluster, new ProducerConfiguration(topic, maxMessageSetSizeInBytes: 1024*1024));
            producer.OnSuccess += e => e.ForEach(sentEvents.OnNext);

            await producer.ConnectAsync();

            // read offsets of empty queue
            var heads = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);
            var tails = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);
            Assert.AreEqual(1, heads.Partitions.Count(), "Expected just one head partition");
            Assert.AreEqual(1, tails.Partitions.Count(), "Expected just one tail partition");
            Assert.AreEqual(0L, heads.NextOffset(heads.Partitions.First()), "Expected start at 0");
            Assert.AreEqual(0L, tails.NextOffset(tails.Partitions.First()), "Expected end at 0");

            // log the broker selected as master
            var brokerMeta = cluster.FindBrokerMetaForPartitionId(topic, heads.Partitions.First());
            _log.Info("Partition Leader is {0}", brokerMeta);


            // saw some inconsistency, so run this a few times.
            const int count = 1100;
            const int loops = 10;
            for (int i = 0; i < loops; i++)
            {
                // NOTE that the configuration for the test machines through vagrant are set to 1MB rolling file segments
                // so we need to generate large messages to force multiple segments to be created.

                // send count messages
                Enumerable.Range(1, count).
                    Select(_ => new Message { Value = new byte[1024] }).
                    ForEach(producer.Send);
                _log.Info("Waiting for {0} sent messages", count);
                await sentEvents.Take(count).ToTask();

                // re-read offsets after messages published
                await Task.Delay(TimeSpan.FromMilliseconds(1000)); // NOTE: There seems to be a race condition on the Kafka broker that the offsets are not immediately available after getting a successful produce response 
                tails = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);
                _log.Info("2:After loop {0} of {1} messages, Next Offset is {2}", i + 1, count, tails.NextOffset(tails.Partitions.First()));
                Assert.AreEqual(count * (i + 1), tails.NextOffset(tails.Partitions.First()), "Expected end at " + count * (i + 1));

            }

            _log.Info("Closing producer");
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            await Task.Delay(TimeSpan.FromSeconds(1));

            // re-read offsets after messages published
            heads = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);
            tails = await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicEnd);

            Assert.AreEqual(1, heads.Partitions.Count(), "Expected just one head partition");
            Assert.AreEqual(1, tails.Partitions.Count(), "Expected just one tail partition");
            Assert.AreEqual(0L, heads.NextOffset(heads.Partitions.First()), "Expected start at 0");
            Assert.AreEqual(count*loops, tails.NextOffset(tails.Partitions.First()), "Expected end at " + count);

            kafka4net.Tracing.EtwTrace.Marker("/ReadOffsets");
        }

        [Test]
        public async void TwoConsumerSubscribersOneBroker()
        {
            kafka4net.Tracing.EtwTrace.Marker("TwoConsumerSubscribersOneBroker");

            var handler = new BufferBlock<ReceivedMessage>();
            var consumer = Consumer.Create(_seedAddresses, "part33").WithAction(handler).Build();
            var msgs = handler.AsObservable().Publish().RefCount();
            var t1 = msgs.TakeUntil(DateTimeOffset.Now.AddSeconds(5)).LastOrDefaultAsync().ToTask();
            var t2 = msgs.TakeUntil(DateTimeOffset.Now.AddSeconds(6)).LastOrDefaultAsync().ToTask();
            //await consumer.IsConnected;
            await Task.WhenAll(new[] { t1, t2 });
            consumer.Dispose();

            kafka4net.Tracing.EtwTrace.Marker("/TwoConsumerSubscribersOneBroker");
        }

        [Test]
        public async void MultipleProducersOneCluster()
        {
            kafka4net.Tracing.EtwTrace.Marker("MultipleProducersOneCluster");

            var cluster = new Cluster(_seedAddresses);
            var topic1 = "topic." + _rnd.Next();
            var topic2 = "topic." + _rnd.Next();

            VagrantBrokerUtil.CreateTopic(topic1, 6, 3);
            VagrantBrokerUtil.CreateTopic(topic2, 6, 3);

            // declare two producers
            var producer1 = new Producer(cluster, new ProducerConfiguration(topic1));
            await producer1.ConnectAsync();

            var producer2 = new Producer(cluster, new ProducerConfiguration(topic2));
            await producer2.ConnectAsync();

            // run them both for a little while (~10 seconds)
            var msgs = await Observable.Interval(TimeSpan.FromMilliseconds(100))
                .Do(l =>
            {
                producer1.Send(new Message {Value = BitConverter.GetBytes(l)});
                producer2.Send(new Message {Value = BitConverter.GetBytes(l)});

            }).Take(100);

            _log.Info("Done Sending, await on producer close.");

            // now stop them.
            await Task.WhenAll(new [] { producer1.CloseAsync(TimeSpan.FromSeconds(5)), producer2.CloseAsync(TimeSpan.FromSeconds(5)) });

            await Task.Delay(TimeSpan.FromSeconds(2));

            // check we got all 100 on each topic.
            _log.Info("Closed Producers. Checking Offsets");
            var topic1Heads = await cluster.FetchPartitionOffsetsAsync(topic1, ConsumerLocation.TopicStart);
            var topic2Heads = await cluster.FetchPartitionOffsetsAsync(topic2, ConsumerLocation.TopicStart);
            var topic1Tails = await cluster.FetchPartitionOffsetsAsync(topic1, ConsumerLocation.TopicEnd);
            var topic2Tails = await cluster.FetchPartitionOffsetsAsync(topic2, ConsumerLocation.TopicEnd);

            Assert.AreEqual(100, topic1Tails.MessagesSince(topic1Heads));
            Assert.AreEqual(100, topic2Tails.MessagesSince(topic2Heads));
            
            kafka4net.Tracing.EtwTrace.Marker("/MultipleProducersOneCluster");
        }

        [Test]
        public async void SchedulerThreadIsIsolatedFromUserCode()
        {
            kafka4net.Tracing.EtwTrace.Marker("SchedulerThreadIsIsolatedFromUserCode");

            const string threadName = "kafka-scheduler";
            _log.Info("Test Runner is using thread {0}", Thread.CurrentThread.Name);

            var topic = "topic." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,6,3);

            var cluster = new Cluster(_seedAddresses);
            await cluster.ConnectAsync();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            await cluster.FetchPartitionOffsetsAsync(topic, ConsumerLocation.TopicStart);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            var topics = await cluster.GetAllTopicsAsync();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            // now create a producer
            var producer = new Producer(cluster, new ProducerConfiguration(topic));
            await producer.ConnectAsync();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            // create a producer that also creates a cluster
            var producerWithCluster = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            await producerWithCluster.ConnectAsync();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            // TODO: Subscribe and check thread on notification observables!

            // run them both for a little while (~5 seconds)
            var msgs = await Observable.Interval(TimeSpan.FromMilliseconds(100))
                .Do(l =>
                {
                    producer.Send(new Message { Value = BitConverter.GetBytes(l) });
                    producerWithCluster.Send(new Message { Value = BitConverter.GetBytes(l) });
                    _log.Debug("After Producer Send using thread {0}", Thread.CurrentThread.Name);

                }).Take(50).ToArray();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            // now consumer(s)
            var handler = new BufferBlock<ReceivedMessage>();
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPositionAtBeginning().
                WithAction(handler).
                Build();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            var msgsRcv = new List<long>();
            var messageSubscription = handler.AsObservable()
                .Do(msg => Assert.AreEqual(threadName, Thread.CurrentThread.Name), exception => Assert.AreEqual(threadName, Thread.CurrentThread.Name), () => Assert.AreEqual(threadName, Thread.CurrentThread.Name))
                .Take(50)
                .TakeUntil(DateTime.Now.AddSeconds(500))
                .ObserveOn(System.Reactive.Concurrency.DefaultScheduler.Instance)
                .Do(msg => Assert.AreNotEqual(threadName, Thread.CurrentThread.Name), exception => Assert.AreNotEqual(threadName, Thread.CurrentThread.Name), () => Assert.AreNotEqual(threadName, Thread.CurrentThread.Name))
                .Subscribe(
                    msg=>
                    {
                        msgsRcv.Add(BitConverter.ToInt64(msg.Value,0));
                        Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);
                        _log.Debug("In Consumer Subscribe OnNext using thread {0}", Thread.CurrentThread.Name);
                    }, exception =>
                    {
                        _log.Debug("In Consumer Subscribe OnError using thread {0} Error: {1}", Thread.CurrentThread.Name, exception.Message);
                        throw exception;
                    }, () =>
                    {
                        Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);
                        _log.Debug("In Consumer Subscribe OnComplete using thread {0}", Thread.CurrentThread.Name);
                    });
            
            await consumer.State.Connected;

            _log.Info("Waitng for consumer to read");
            await Task.Delay(TimeSpan.FromSeconds(6));
            _log.Debug("After Consumer Subscribe using thread {0}", Thread.CurrentThread.Name);
            consumer.Dispose();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            Assert.AreEqual(msgs.Length, msgsRcv.Count);

            messageSubscription.Dispose();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            // now close down
            await producer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Debug("After Consumer Close using thread {0}", Thread.CurrentThread.Name);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            await producerWithCluster.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Debug("After Producer Subscribe using thread {0}", Thread.CurrentThread.Name);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            await cluster.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Debug("After Cluster Close using thread {0}", Thread.CurrentThread.Name);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            kafka4net.Tracing.EtwTrace.Marker("/SchedulerThreadIsIsolatedFromUserCode");
        }

        [Test]
        public async void SlowConsumer()
        {
            //
            // 1. Create a topic with 100K messages.
            // 2. Create slow consumer and start consuming at rate 1msg/sec
            // 3. How to validate that fetcher has been paused and memory did not exceed sertail limit???
            //
            var topic = "test32." + _rnd.Next();

            await FillOutQueue(topic, (int)100e3);

            Action<ReceivedMessage> handler = msg => {
                var str = Encoding.UTF8.GetString(msg.Value);
                _log.Debug("Received msg '{0}'", str);
                Thread.Sleep(100);
            };

            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPositionAtBeginning().
                WithMaxBytesPerFetch(1024).
                WithAction(handler).
                Build();

            var readWaiter = new SemaphoreSlim(0, 1);
            await consumer.State.Connected;

            _log.Debug("Waiting for reader");
            await readWaiter.WaitAsync();
            _log.Debug("Reader complete");
        }

        [Test]
        [ExpectedException(typeof(AggregateException))]
        public async void InvalidOffsetShouldLogErrorAndStopFetching()
        {
            var count = 100;
            var topic = "test11."+_rnd.Next();
            await FillOutQueue(topic, count);

            var badPartitionMap = new Dictionary<int, long>{{0, -1}};
            var offsets = new TopicPartitionOffsets(topic, badPartitionMap);
            var handler = new BufferBlock<ReceivedMessage>();
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPosition(offsets).
                WithAction(handler).
                Build();
            await consumer.State.Connected;
            try
            {
                await handler.AsObservable().FirstOrDefaultAsync();
            }
            catch (AggregateException e)
            {
                Assert.IsTrue(e.InnerExceptions.Count == 1);
                Assert.IsTrue(e.InnerExceptions[0] is PartitionFailedException);
                throw;
            }
            await consumer.State.Connected;
            
            _log.Info("Done");
        }

        private async Task FillOutQueue(string topic, int count)
        {
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic, TimeSpan.FromSeconds(20)));

            _log.Debug("Connecting producer");
            await producer.ConnectAsync();

            _log.Info("Starting sender");
            var sender = Observable.Range(1, count).
                Select(i => string.Format("msg {0} {1}", i, Guid.NewGuid())).
                Synchronize().
                Select(msg => new Message
                {
                    Value = Encoding.UTF8.GetBytes(msg)
                }).
                Publish().RefCount();
            sender.Subscribe(producer.Send);

            _log.Debug("Waiting for sender");
            await sender.LastOrDefaultAsync().ToTask();
            _log.Debug("Waiting for producer complete");
            await producer.CloseAsync(TimeSpan.FromSeconds(4));
            _log.Debug("Producer complete");
        }

        [Test]
        public async void SpeedTest()
        {
            int _pageViewBatchSize = 10 * 1000;
            TimeSpan _batchTime = TimeSpan.FromSeconds(10);
            var count = 0L;
            var lastCount = count;

            var handler = new BatchBlock<ReceivedMessage>(_pageViewBatchSize, new GroupingDataflowBlockOptions { BoundedCapacity = 1});
                //.WithTimeout(_batchTime);
            var handler2 = new ActionBlock<ReceivedMessage[]>(batch => Interlocked.Add(ref count, batch.Length));
            handler.LinkTo(handler2, new DataflowLinkOptions { PropagateCompletion = true });
            var consumer = Consumer.Create("kafkadev-01.lv.ntent.com", "vsw.avrodto.addelivery.activitylogging.pageview").
                WithStartPositionAtBeginning().
                WithAction(handler).
                Build();
            
            var interval = TimeSpan.FromSeconds(5);
            var lastTime = DateTime.Now;
            Observable.Interval(interval).Subscribe(_ => {
                var c = count;
                var now = DateTime.Now;
                var time = now - lastTime;
                var speed = (c - lastCount)/time.TotalSeconds;
                lastCount = c;
                lastTime = now;
                Console.WriteLine("{0}msg/sec #{1}", speed, c);
            });

            await consumer.State.Connected;

            await Task.Delay(TimeSpan.FromSeconds(30));
            Console.WriteLine("Complete {0}", count);
        }

        [Test]
        [Timeout(10*1000)]
        [ExpectedException(typeof(BrokerException))]
        public async void InvalidDnsShouldThrowException()
        {
            var consumer = Consumer.Create("no.such.name.123.org", "some.topic").WithAction(_ => { }).Build();
            await consumer.State.Connected;
        }

        [Test]
        [Timeout(10 * 1000)]
        public async void OneInvalidAndOtherValidDnsShouldNotThrow()
        {
            var seed = "no.such.name.123.org,"+_seedAddresses;
            var topic = "some.topic";
            var consumer = Consumer.Create(seed, topic).WithAction(_ => { }).Build();
            await consumer.State.Connected;
            consumer.Dispose();
            await consumer.State.Closed;
        }


        [Test]
        [ExpectedException(typeof(WorkingThreadHungException))]
        public async void SimulateSchedulerHanging()
        {
            var topic = "topic11."+_rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 1, 1);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic, batchFlushSize: 2));
            await producer.ConnectAsync();
            // hung upon 1st confirmation
            int c = 0;
            producer.OnSuccess += messages =>
            {
                if(c++ == 1)
                    new ManualResetEvent(false).WaitOne();
            };

            var ctx = SynchronizationContext.Current;
            producer.OnPermError += (exception, messages) => ctx.Post(d => { throw exception; }, null);

            var source = Observable.Interval(TimeSpan.FromSeconds(1)).Take(1000).Publish();
            source.Connect();
            
            source.//Do(i => {if(i == 2) producer.DebugHangScheduler();}).
            Select(i => new Message{Value = BitConverter.GetBytes(i)}).
            Subscribe(producer.Send);

            await source;
        }

        [Test]
        public async void SimulateLongBufferedMessageHandling()
        {
            var count = 2000;
            var topic = "topic11." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 1, 1);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic, batchFlushSize: 2));
            await producer.ConnectAsync();

            var src = Observable.Interval(TimeSpan.FromMilliseconds(10)).Take(count).
                Select(i => new Message { Value = BitConverter.GetBytes((int)i) }).Publish().RefCount();
            src.Subscribe(producer.Send);
            await src;

            await Task.Delay(200);
            await producer.CloseAsync(TimeSpan.FromSeconds(5));

            var handler = new BatchBlock<ReceivedMessage>(count/4);//.WithTimeout(TimeSpan.FromSeconds(1))

            _log.Debug("Start consumer");
            var consumer = Consumer.Create(_seedAddresses, topic).
                WithStartPositionAtBeginning().
                WithAction(handler).
                Build();
            
            var stream = handler.AsObservable().
                Select(i =>
                {
                    _log.Debug("Handling batch {0} ...", i.Length);
                    Thread.Sleep(65*1000);
                    _log.Debug("Complete batch");
                    return i.Length;
                }).
                Scan(0, (i, i1) =>
                {
                    _log.Debug("Scnning {0} {1}", i, i1);
                    return i + i1;
                }).
                Do(i => _log.Debug("Do {0}", i)).
                Where(i => i == count).FirstAsync().ToTask();
            await consumer.State.Connected;

            _log.Debug("Awaiting for consumer");
            var count2 = await stream;
            consumer.Dispose();
            Assert.AreEqual(count, count2);
            _log.Info("Complete");
        }

        [Ignore]
        public void TestSerializerTmp()
        {
            var txt = File.ReadAllText(@"c:\tmp\dump.txt").Trim();
            var buff = new byte[txt.Length / 2];
            for (int i = 0; i < txt.Length; i += 2)
            {
                var dd = txt.Substring(i, 2);
                var b = byte.Parse(dd, NumberStyles.HexNumber);
                buff[i / 2] = b;
            }

            var response = kafka4net.Protocols.Serializer.DeserializeFetchResponse(buff);
            Console.WriteLine(response.Topics);
        }

        // if last leader is down, all in-buffer messages are errored and the new ones
        // are too.

        // How to test timeout error?

        // shutdown dirty: connection is off while partially saved. Make sure that 
        // message is either committed or errored

        // If connecting async, messages are saved and than sent

        // if connecting async but never can complete, timeout triggers, and messages are errored

        // if one broker refuses connection, another one will connect and function

        // if one broker hangs on connect, client will be ready as soon as connected via another broker

        // Short disconnect (within timeout) wont lose any messages and will deliver all of them.
        // Temp error will be triggered

        // Parallel producers send messages to proper topics

        // Test non-keyed messages. What to test?

        // Big message batching does not cause too big payload exception

        // when kafka delete is implemented, test deleting topic cause delete metadata in driver
        // and proper message error

        // Analize correlation example
        // C:\funprojects\rx\Rx.NET\Samples\EventCorrelationSample\EventCorrelationSample\Program.cs 

        // Adaptive timeout and buffer size on fetch?

        // If connection lost, recover. But if too frequent, do not loop infinitely 
        // establishing connection but fail permanently.

        // When one consumer fails and recovers (leader changed), another, inactive one will
        // update its connection mapping just by listening to changes in routing table and not
        // though error and recovery, when it becomes active.

        // The same key sends message to be in the same partition

        // If 2 consumers subscribed, and than unsubscribed, fetcher must stop pooling.

        // Kafka bug? Fetch to broker that has topic but not the partition, returns no error for partition, but [-1,-1,0] offsets

        // Do I need SynchronizationContext in addition to EventLoopScheduler when using async?

        // When server close tcp socket, Fetch will wait until timeout. It would be better to 
        // react to connection closure immediatelly.

        // Sending messages to Producer after shutdown causes error

        // Clean shutdown doe not produce shutdown error callbacks

        // OnSuccess is fired if topic was autocreated, there was no errors, there were 1 or more errors with leaders change

        // For same key, order of the messages is preserved

    }
}
