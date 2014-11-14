using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using kafka4net;
using kafka4net.Protocols.Requests;
using kafka4net.ConsumerImpl;
using kafka4net.Utils;
using NLog;
using NLog.Config;
using NLog.Targets;
using NUnit.Framework;
using Logger = kafka4net.Logger;

namespace tests
{
    [TestFixture]
    class RecoveryTest
    {
        Random _rnd = new Random();
        //string _seedAddresses = "192.168.56.10,192.168.56.20";
        private const string _seedAddresses = "192.168.56.10";
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

            consoleTarget.Layout = "${date:format=HH\\:mm\\:ss.fff} ${level} [${threadname}:${threadid}] ${logger} ${message} ${exception:format=tostring}";
            fileTarget.FileName = "${basedir}../../../../log.txt";
            fileTarget.Layout = "${longdate} ${level} [${threadname}:${threadid}] ${logger:shortName=true} ${message} ${exception:format=tostring,stacktrace:innerFormat=tostring,stacktrace}";

            config.LoggingRules.Add(new LoggingRule("tests.*", LogLevel.Debug, consoleTarget) { Final = true });
            var rule = new LoggingRule("*", LogLevel.Info, consoleTarget);
            rule.Targets.Add(fileTarget);
            rule.Final = true;


            var r1 = new LoggingRule("kafka4net.Internal.PartitionRecoveryMonitor", LogLevel.Info, fileTarget) { Final = true };
            r1.Targets.Add(consoleTarget);
            config.LoggingRules.Add(r1);

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

            var logger = LogManager.GetLogger("Main");
            logger.Debug("=============== Starting =================");

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

            TaskScheduler.UnobservedTaskException += (sender, args) => logger.Error("Unhandled task exception", (Exception)args.Exception);
            AppDomain.CurrentDomain.UnhandledException += (sender, args) => logger.Error("Unhandled exception", (Exception)args.ExceptionObject);

            // make sure brokers are up from last run
            VagrantBrokerUtil.RestartBrokers();
        }

        [TearDown]
        public void RestartBrokers()
        {
            VagrantBrokerUtil.RestartBrokers();
        }

        // If failed to connect, messages are errored

        // if topic does not exists, it is created when producer connects
        [Test]
        public async void TopicIsAutocreatedByProducer()
        {
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
            await producer.Close(TimeSpan.FromSeconds(10));

            //
            // Validate by reading published messages
            //
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, maxWaitTimeMs: 1000, minBytesPerFetch: 1, startLocation: ConsumerStartLocation.TopicHead));
            await consumer.ConnectAsync();
            var receivedTxt = new List<string>();
            var consumerSubscription = consumer.OnMessageArrived.
                Select(m => Encoding.UTF8.GetString(m.Value)).
                Do(m => _log.Info("Received {0}", m)).
                Synchronize(). // protect receivedTxt
                Do(receivedTxt.Add).
                Subscribe();

            _log.Debug("Waiting for consumer");
            await consumer.OnMessageArrived.Take(producedCount).TakeUntil(DateTimeOffset.Now.AddSeconds(5)).LastOrDefaultAsync().ToTask();

            Assert.AreEqual(producedCount, receivedTxt.Count, "Did not received all messages");
            Assert.IsTrue(receivedTxt.All(m => m == "la-la-la"), "Unexpected message content");

            consumerSubscription.Dispose();
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
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

        // if leader goes down, messages keep being accepted 
        // and are committed (can be read) within (5sec?)
        // Also, order of messages is preserved
        [Test]
        public async void LeaderDownProducerAndConsumerRecovery()
        {
            const string topic = "part33";
            var sent = new List<string>();
            var confirmedSent1 = new List<string>();

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic))
            {
                OnSuccess = msgs =>
                {
                    msgs.ForEach(msg => confirmedSent1.Add(Encoding.UTF8.GetString(msg.Value)));
                    _log.Debug("Sent {0} messages", msgs.Length);
                }
            };
            await producer.ConnectAsync();

            if (!producer.Cluster.GetAllTopics().Contains(topic))
                VagrantBrokerUtil.CreateTopic(topic, 3, 3);

            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic));
            await consumer.ConnectAsync();

            const int postCount = 100;
            const int postCount2 = 50;

            //
            // Read messages
            //
            var received = new List<ReceivedMessage>();
            var receivedEvents = new ReplaySubject<ReceivedMessage>();
            var consumerSubscription = consumer.OnMessageArrived.
                Synchronize().
                Subscribe(msg =>
                {
                    received.Add(msg);
                    receivedEvents.OnNext(msg);
                    _log.Debug("Received {0}/{1}", Encoding.UTF8.GetString(msg.Value), received.Count);
                });

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
            await producer.Close(TimeSpan.FromSeconds(5));

            _log.Info("Waiting 4sec for remaining messages");
            await Task.Delay(TimeSpan.FromSeconds(4)); // if unexpected messages arrive, let them in to detect failure

            _log.Info("Waiting for consumer.CloseAsync");
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
            consumerSubscription.Dispose();

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
        }

        [Test]
        public async void ListenerOnNonExistentTopicWaitsForTopicCreation()
        {
            const int numMessages = 400;
            var topic = "topic." + _rnd.Next();
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses,topic,ConsumerStartLocation.TopicHead));
            await consumer.ConnectAsync();
            var cancelSubject = new Subject<bool>();
            var receivedValuesTask = consumer.OnMessageArrived
                .Select(msg=>BitConverter.ToInt32(msg.Value, 0))
                .Do(val=>_log.Info("Received: {0}", val))
                .Take(numMessages)
                .TakeUntil(cancelSubject)
                .ToList().ToTask();
            //receivedValuesTask.Start();

            // wait a couple seconds for things to "stabilize"
            await Task.Delay(TimeSpan.FromSeconds(4));

            // now produce 400 messages
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));
            await producer.ConnectAsync();
            Enumerable.Range(1, numMessages).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            await Task.Delay(TimeSpan.FromSeconds(2));
            await producer.Close(TimeSpan.FromSeconds(5));

            // wait another little while, and stop the producer.
            await Task.Delay(TimeSpan.FromSeconds(2));

            cancelSubject.OnNext(true);

            var receivedValues = await receivedValuesTask;
            Assert.AreEqual(numMessages,receivedValues.Count);

        }


        /// <summary>
        /// Test listener and producer recovery together
        /// </summary>
        [Test]
        public async void ProducerAndListenerRecoveryTest()
        {
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

            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, ConsumerStartLocation.TopicHead, maxBytesPerFetch: 4*8));
            await consumer.ConnectAsync();
            var current =0;
            var received = new ReplaySubject<ReceivedMessage>();
            var consumerSubscription = consumer.OnMessageArrived.
                Subscribe(async msg => {
                    current++;
                    if (current == 18)
                    {
                        await Task.Factory.StartNew(() => VagrantBrokerUtil.StopBrokerLeaderForPartition(consumer.Cluster, consumer.Topic, msg.Partition), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                    }
                    received.OnNext(msg);
                    _log.Info("Got: {0}", BitConverter.ToInt32(msg.Value, 0));
                });

            _log.Info("Waiting for receiver complete");
            var receivedList = await received.Select(msg => BitConverter.ToInt32(msg.Value, 0)).Take(count).TakeUntil(DateTime.Now.AddSeconds(60)).ToList().ToTask();

            // get the offsets for comparison later
            var parts = await consumer.Cluster.FetchPartitionOffsetsAsync(topic);

            _log.Info("Done waiting for receiver. Closing producer.");
            await producer.Close(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed, disposing consumer subscription.");
            consumerSubscription.Dispose();
            _log.Info("Consumer subscription disposed. Closing consumer.");
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Info("Consumer closed.");

            if (sentList.Count != receivedList.Count)
            {
                // log some debug info.
                _log.Error("Did not receive all messages. Messages received: {0}",string.Join(",",receivedList.OrderBy(i=>i)));
                _log.Error("Did not receive all messages. Messages sent but NOT received: {0}", string.Join(",", sentList.Except(receivedList).OrderBy(i => i)));

                _log.Error("Sum of offsets fetched: {0}", parts.Select(p => p.Tail-p.Head).Sum());
                _log.Error("Offsets fetched: [{0}]", string.Join(",", parts.Select(p => p.ToString())));
            }

            Assert.AreEqual(sentList.Count, receivedList.Count);

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
            const int count = 200;
            var topic = "part62." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic, 6, 2);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic));

            _log.Debug("Connecting");
            await producer.ConnectAsync();

            _log.Debug("Filling out {0}", topic);
            // when we get a confirm back, add to list actually sent.
            var actuallySentList = new List<int>(200);
            producer.OnSuccess += msgs => actuallySentList.AddRange(msgs.Select(msg => BitConverter.ToInt32(msg.Value, 0)));

            var sentList = await Observable.Interval(TimeSpan.FromMilliseconds(100))
                .Select(l => (int)l)
                .Do(l => { if(l==30) Task.Factory.StartNew(() => VagrantBrokerUtil.StopBroker("broker2"), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default); })
                .Select(i => new Message { Value = BitConverter.GetBytes(i) })
                .Take(count)
                .Do(producer.Send)
                .Select(msg => BitConverter.ToInt32(msg.Value, 0))
                .ToList();


            _log.Info("Done waiting for sending. Closing producer.");
            await producer.Close(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed.");

            //
            // Check length of result topic
            //
            var c2 = new Cluster(_seedAddresses);
            await c2.ConnectAsync();
            var parts = await c2.FetchPartitionOffsetsAsync(topic);

            _log.Info("Sum of offsets: {0}", parts.Select(p => p.Tail - p.Head).Sum());
            _log.Info("Offsets: [{0}]", string.Join(",", parts.Select(p => p.ToString())));

            //
            if (sentList.Count != actuallySentList.Count)
            {
                // log some debug info.
                _log.Error("Did not send all messages. Messages sent but NOT acknowledged: {0}", string.Join(",", sentList.Except(actuallySentList).OrderBy(i => i)));
            }

            Assert.AreEqual(sentList.Count, actuallySentList.Count, "Actually sent");
            Assert.AreEqual(sentList.Count, parts.Select(p => p.Tail - p.Head).Sum(), "Offsets");

        }

        /// <summary>
        /// Test just listener recovery isolated from producer
        /// </summary>
        [Test]
        public async void ListenerRecoveryTest()
        {
            const int count = 8000;
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

            var parts = await producer.Cluster.FetchPartitionOffsetsAsync(topic);

            _log.Info("Done sending messages. Closing producer.");
            await producer.Close(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed, starting consumer subscription.");

            var messagesInTopic = (int)parts.Select(p => p.Tail - p.Head).Sum();
            _log.Info("Topic offsets indicate producer sent {0} messages.", messagesInTopic);


            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, ConsumerStartLocation.TopicHead, maxBytesPerFetch: 4 * 8));
            await consumer.ConnectAsync();
            var current = 0;
            var received = new ReplaySubject<ReceivedMessage>();
            var consumerSubscription = consumer.OnMessageArrived.
                Subscribe(async msg =>
                {
                    current++;
                    if (current == 18)
                    {
                        await Task.Factory.StartNew(() => VagrantBrokerUtil.StopBrokerLeaderForPartition(consumer.Cluster, consumer.Topic, msg.Partition), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                    }
                    received.OnNext(msg);
                    //_log.Info("Got: {0}", BitConverter.ToInt32(msg.Value, 0));
                });

            _log.Info("Waiting for receiver complete");
            var receivedList = await received.Select(msg => BitConverter.ToInt32(msg.Value, 0)).Take(messagesInTopic).TakeUntil(DateTime.Now.AddSeconds(60)).ToList().ToTask();

            parts = await consumer.Cluster.FetchPartitionOffsetsAsync(topic);

            _log.Info("Receiver complete. Disposing Subscription");
            consumerSubscription.Dispose();
            _log.Info("Consumer subscription disposed. Closing consumer.");
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Info("Consumer closed.");

            _log.Error("Sum of offsets fetched: {0}", parts.Select(p => p.Tail - p.Head).Sum());
            _log.Error("Offsets fetched: [{0}]", string.Join(",", parts.Select(p => p.ToString())));

            if (messagesInTopic != receivedList.Count)
            {
                // log some debug info.
                _log.Error("Did not receive all messages. Messages sent but NOT received: {0}", string.Join(",", sentList.Except(receivedList).OrderBy(i => i)));

            }

            Assert.AreEqual(messagesInTopic, receivedList.Count);

        }

        /// <summary>
        /// Set long batching period for producer (20 sec) and make sure that shutdown flushes
        /// buffered data.
        /// </summary>
        [Test]
        public async void CleanShutdownTest()
        {
            const string topic = "shutdown.test";

            // set producer long batching period, 20 sec
            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic, TimeSpan.FromSeconds(20), int.MaxValue));

            _log.Debug("Connecting");
            await producer.ConnectAsync();

            // start listener at the end of queue and accumulate received messages
            var received = new HashSet<string>();
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, maxWaitTimeMs: 30 * 1000));
            _log.Info("Connecting consumer");
            await consumer.ConnectAsync();
            _log.Info("Subscribing to consumer");
            var consumerSubscription = consumer.OnMessageArrived
                                        .Select(msg => Encoding.UTF8.GetString(msg.Value))
                                        .Subscribe(m => received.Add(m));
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
            await producer.Close(TimeSpan.FromSeconds(4));

            // how to make sure nothing is sent after shutdown? listen to logger?  have connection events?

            // wait for 5sec for receiver to get all the messages
            _log.Info("Waiting for consumer to fetch");
            await Task.Delay(5000);
            _log.Info("Disposing consumer subscription");
            consumerSubscription.Dispose();
            _log.Info("Closing consumer");
            await consumer.CloseAsync(TimeSpan.FromSeconds(4));
            _log.Info("Closed consumer");

            // assert we received all the messages

            Assert.AreEqual(sent.Count, received.Count, string.Format("Sent and Receved size differs. Sent: {0} Recevied: {1}", sent.Count, received.Count));
            // compare sets and not lists, because of 2 partitions, send order and receive orser are not the same
            Assert.True(received.SetEquals(sent), "Sent and Received set differs");
        }

        //[Test]
        //public void DirtyShutdownTest()
        //{

        //}

        [Test]
        public async void ConsumerFollowsRebalancingPartitions()
        {

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
            Assert.True(partitionMeta.GroupBy(p=>p.Leader).Count()==1);

            // now publish messages
            const int count = 48000;
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

            var parts = await producer.Cluster.FetchPartitionOffsetsAsync(topic);

            _log.Info("Done sending messages. Closing producer.");
            await producer.Close(TimeSpan.FromSeconds(5));
            _log.Info("Producer closed, starting consumer subscription.");

            var messagesInTopic = (int)parts.Select(p => p.Tail - p.Head).Sum();
            _log.Info("Topic offsets indicate producer sent {0} messages.", messagesInTopic);


            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, ConsumerStartLocation.TopicHead, maxBytesPerFetch: 4 * 8));
            await consumer.ConnectAsync();
            var current = 0;
            var received = new ReplaySubject<ReceivedMessage>();
            var consumerSubscription = consumer.OnMessageArrived.
                Subscribe(async msg =>
                {
                    current++;
                    if (current == 18)
                    {
                        await Task.Factory.StartNew(VagrantBrokerUtil.RebalanceLeadership, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                    }
                    received.OnNext(msg);
                    //_log.Info("Got: {0}", BitConverter.ToInt32(msg.Value, 0));
                });

            _log.Info("Waiting for receiver complete");
            var receivedList = await received.Select(msg => BitConverter.ToInt32(msg.Value, 0)).Take(messagesInTopic).TakeUntil(DateTime.Now.AddSeconds(120)).ToList().ToTask();

            parts = await consumer.Cluster.FetchPartitionOffsetsAsync(topic);

            _log.Info("Receiver complete. Disposing Subscription");
            consumerSubscription.Dispose();
            _log.Info("Consumer subscription disposed. Closing consumer.");
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Info("Consumer closed.");

            _log.Error("Sum of offsets fetched: {0}", parts.Select(p => p.Tail - p.Head).Sum());
            _log.Error("Offsets fetched: [{0}]", string.Join(",", parts.Select(p => p.ToString())));

            if (messagesInTopic != receivedList.Count)
            {
                // log some debug info.
                _log.Error("Did not receive all messages. Messages sent but NOT received: {0}", string.Join(",", sentList.Except(receivedList).OrderBy(i => i)));

            }

            Assert.AreEqual(messagesInTopic, receivedList.Count);


        }

        [Test]
        public async void KeyedMessagesPreserveOrder()
        {
            // create a topic with 3 partitions
            var topicName = "part33." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topicName, 3, 3);
            
            // create listener in a separate connection/broker
            var receivedMsgs = new List<ReceivedMessage>();
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topicName));
            await consumer.ConnectAsync();
            var consumerSubscription = consumer.OnMessageArrived.Synchronize().Subscribe(msg =>
            {
                lock (receivedMsgs)
                {
                    receivedMsgs.Add(msg);
                    //_log.Info("Received '{0}'/{1}/{2}", Encoding.UTF8.GetString(msg.Value), msg.Partition, BitConverter.ToInt32(msg.Key, 0));
                }
            });

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
                            //_log.Info("Sent '{0}'/{1}", msg.Item3, BitConverter.ToInt32(msg.Item1.Key, 0));
                        }
                    })
                ).
                ToArray();

            // wait for around 10K messages (10K/(10*10) = 100sec) and close producer
            _log.Info("Waiting for producer to produce enough...");
            Thread.Sleep(100*1000);
            _log.Info("Closing senders intervals");
            senders.ForEach(s => s.Dispose());
            _log.Info("Closing producer");
            await producer.Close(TimeSpan.FromSeconds(5));

            // wait for 3 sec for listener to catch up
            _log.Info("Waiting for additional 3sec");
            Thread.Sleep(3*1000);

            _log.Info("Disposing consumer");
            consumerSubscription.Dispose();
            _log.Info("Closing consumer");
            await consumer.CloseAsync(TimeSpan.FromSeconds(4));
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
        }

        // take 100 items starting from the ones which differ
        static IEnumerable<Tuple<string, string>> DumpOutOfOrder(IEnumerable<string> l1, IEnumerable<string> l2)
        {
            return l1.Zip(l2, (l, r) => new {l, r}).
                SkipWhile(_ => _.l == _.r).Take(100).
                Select(_ => Tuple.Create(_.l, _.r));
        }

        // explicit offset works
        [Test]
        public async void ExplicitOffset()
        {
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
            await producer.Close(TimeSpan.FromSeconds(5));

            var offsetFetchCluster = new Cluster(_seedAddresses);
            await offsetFetchCluster.ConnectAsync();

            // consume tail-300 for each partition
            var offsets = (await offsetFetchCluster.FetchPartitionOffsetsAsync(topic)).ToDictionary(p => p.Partition);
            
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, ConsumerStartLocation.SpecifiedLocations, partitionOffsetProvider: p => offsets[p].Tail - 300));
            await consumer.ConnectAsync();
            var messages = consumer.OnMessageArrived.
                GroupBy(m => m.Partition).Replay();
            messages.Connect();

            var consumerSubscription = messages.Subscribe(p => p.Take(10).Subscribe(
                m => _log.Debug("Got message {0}/{1}", m.Partition, BitConverter.ToInt32(m.Value, 0)),
                e => _log.Error("Error", e),
                () => _log.Debug("Complete part {0}", p.Key)
            ));

            // wait for 3 partitions to arrrive and every partition to read at least 100 messages
            await messages.Select(g => g.Take(100)).Take(3).ToTask();

            consumerSubscription.Dispose();
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
        }

        // can read from the head of queue
        [Test]
        public async void ReadFromHead()
        {
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
            await producer.Close(TimeSpan.FromSeconds(5));

            // read starting from the head
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, ConsumerStartLocation.TopicHead));
            await consumer.ConnectAsync();
            var count2 = consumer.OnMessageArrived.TakeUntil(DateTimeOffset.Now.AddSeconds(5))
                //.Do(val=>_log.Info("received value {0}", BitConverter.ToInt32(val.Value,0)))
                .Count().ToTask();
            Assert.AreEqual(count, await count2);
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

        // Create a new 1-partition topic and sent 100 messages.
        // Read offsets, they should be [0, 100]
        [Test]
        public async void ReadOffsets()
        {
            var sentEvents = new Subject<Message>();
            var topic = "part12." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,1,2);

            var producer = new Producer(_seedAddresses, new ProducerConfiguration(topic)) { OnSuccess = e => e.ForEach(sentEvents.OnNext) };
            await producer.ConnectAsync();

            // read offsets of empty queue
            var parts = await producer.Cluster.FetchPartitionOffsetsAsync(topic);
            Assert.AreEqual(1, parts.Length, "Expected just one partition");
            Assert.AreEqual(0L, parts[0].Head, "Expected start at 0");
            Assert.AreEqual(0L, parts[0].Tail, "Expected end at 0");


            // send 100 messages
            Enumerable.Range(1, 100).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);
            _log.Info("Waiting for 100 sent messages");
            sentEvents.Subscribe(msg => _log.Debug("Sent {0}", BitConverter.ToInt32(msg.Value, 0)));
            await sentEvents.Take(100).ToTask();

            // re-read offsets after messages published
            parts = await producer.Cluster.FetchPartitionOffsetsAsync(topic);

            _log.Info("Closing producer");
            await producer.Close(TimeSpan.FromSeconds(5));

            Assert.AreEqual(1, parts.Length, "Expected just one partition");
            Assert.AreEqual(0, parts[0].Partition, "Expected the only partition with Id=0");
            Assert.AreEqual(0L, parts[0].Head, "Expected start at 0");
            Assert.AreEqual(100L, parts[0].Tail, "Expected end at 100");
        }

        [Test]
        public async void TwoConsumerSubscribersOneBroker()
        {
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, "part33"));
            await consumer.ConnectAsync();
            var t1 = consumer.OnMessageArrived.TakeUntil(DateTimeOffset.Now.AddSeconds(5)).LastOrDefaultAsync().ToTask();
            var t2 = consumer.OnMessageArrived.TakeUntil(DateTimeOffset.Now.AddSeconds(6)).LastOrDefaultAsync().ToTask();
            await Task.WhenAll(new[] { t1, t2 });
            await consumer.CloseAsync(TimeSpan.FromSeconds(2));
        }

        [Test]
        public async void MultipleProducersOneCluster()
        {
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

            // now stop them.
            await Task.WhenAll(new [] { producer1.Close(TimeSpan.FromSeconds(5)), producer2.Close(TimeSpan.FromSeconds(5)) });

            // check we got all 100 on each topic.
            var topic1Offsets = await cluster.FetchPartitionOffsetsAsync(topic1);
            var topic2Offsets = await cluster.FetchPartitionOffsetsAsync(topic2);

            Assert.AreEqual(topic1Offsets.Sum(o => o.Tail - o.Head), 100);
            Assert.AreEqual(topic2Offsets.Sum(o => o.Tail - o.Head), 100);

        }

        [Test]
        public async void SchedulerThreadIsIsolatedFromUserCode()
        {
            const string threadName = "kafka-scheduler";
            _log.Info("Test Runner is using thread {0}", Thread.CurrentThread.Name);

            var topic = "topic." + _rnd.Next();
            VagrantBrokerUtil.CreateTopic(topic,6,3);

            var cluster = new Cluster(_seedAddresses);
            await cluster.ConnectAsync();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            await cluster.FetchPartitionOffsetsAsync(topic);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            var topics = cluster.GetAllTopics();
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
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses,topic,ConsumerStartLocation.TopicHead));
            await consumer.ConnectAsync();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);


            var msgsRcv = new List<long>();
            var messageSubscription = consumer.OnMessageArrived
                .Do(msg => Assert.AreNotEqual(threadName, Thread.CurrentThread.Name), exception => Assert.AreNotEqual(threadName, Thread.CurrentThread.Name), () => Assert.AreNotEqual(threadName, Thread.CurrentThread.Name))
                .Take(50)
                .TakeUntil(DateTime.Now.AddSeconds(500))
                .Subscribe(
                    msg=>
                    {
                        msgsRcv.Add(BitConverter.ToInt64(msg.Value,0));
                        Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);
                        _log.Debug("In Consumer Subscribe OnNext using thread {0}", Thread.CurrentThread.Name);
                    }, exception =>
                    {
                        _log.Debug("In Consumer Subscribe OnError using thread {0}", Thread.CurrentThread.Name);
                        throw exception;
                    }, () =>
                    {
                        Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);
                        _log.Debug("In Consumer Subscribe OnComplete using thread {0}", Thread.CurrentThread.Name);
                    });

            await Task.Delay(TimeSpan.FromSeconds(6));
            _log.Debug("After Consumer Subscribe using thread {0}", Thread.CurrentThread.Name);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            Assert.AreEqual(msgs.Length, msgsRcv.Count);

            messageSubscription.Dispose();
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            // now close down
            await producer.Close(TimeSpan.FromSeconds(5));
            _log.Debug("After Consumer Close using thread {0}", Thread.CurrentThread.Name);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            await producerWithCluster.Close(TimeSpan.FromSeconds(5));
            _log.Debug("After Producer Subscribe using thread {0}", Thread.CurrentThread.Name);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

            await cluster.CloseAsync(TimeSpan.FromSeconds(5));
            _log.Debug("After Cluster Close using thread {0}", Thread.CurrentThread.Name);
            Assert.AreNotEqual(threadName, Thread.CurrentThread.Name);

        }

        // if last leader is down, all in-buffer messages are errored and the new ones
        // are too.

        // How to test timeout error?

        // shutdown:
        // swamp with messages and make sure it flushes cleanly (can read upon new connect)

        // shutdown dirty: connection is off while partially saved. Make sure that 
        // message is either committed or errored

        // if created disconnected, send cause exception

        // if connecting syncronously, sending causes an exception.

        // If connecting async, messages are saved and than sent

        // if connecting async but never can complete, timeout triggers, and messages are errored

        // if one broker refuses connection, another one will connect and function

        // if one broker hangs on connect, client will be ready as soon as connected via another broker

        // Short disconnect (within timeout) wont lose any messages and will deliver all of them.
        // Temp error will be triggered

        // Parallel producers send messages to proper topics

        // Test keyed messages. What to test, sequence?

        // Test non-keyed messages. What to test?

        // Big message batching does not cause too big payload exception

        // TODO: change MessageData.Value to byte[]

        // when kafka delete is implemented, test deleting topic cause delete metadata in driver
        // and proper message error

        // Analize correlation example
        // C:\funprojects\rx\Rx.NET\Samples\EventCorrelationSample\EventCorrelationSample\Program.cs 

        // Fetch API: "partial message at the end of the message set"
        // Q: How to test this???
        // A: by setting max bytes parameter.

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
