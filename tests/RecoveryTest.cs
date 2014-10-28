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
        string _seedAddresses = "192.168.56.10";
        readonly List<string> _stoppedBrokers = new List<string>();
        readonly string _part33CreateScript = "ssh -c '/opt/kafka_2.10-0.8.1.1/bin/kafka-topics.sh --create --topic part33 --partitions 3 --replication-factor 2 --zookeeper 192.168.56.2' broker1";
        static readonly NLog.Logger _log = NLog.LogManager.GetCurrentClassLogger();

        [SetUp]
        public void Setup()
        {
            // NLog
            var config = new LoggingConfiguration();
            var consoleTarget = new ColoredConsoleTarget();
            config.AddTarget("console", consoleTarget);
            var fileTarget = new FileTarget();
            config.AddTarget("file", fileTarget);

            consoleTarget.Layout = "${date:format=HH\\:MM\\:ss} ${level} [${threadname}:${threadid}] ${logger} ${message} ${exception:format=tostring}";
            fileTarget.FileName = "${basedir}../../../../log.txt";
            fileTarget.Layout = "${longdate} ${level} [${threadname}:${threadid}] ${logger:shortName=true} ${message} ${exception:format=tostring,stacktrace:innerFormat=tostring,stacktrace}";

            // disable Transport noise
            // config.LoggingRules.Add(new LoggingRule("kafka4net.Protocol.Transport", LogLevel.Info, fileTarget) { Final = true});
            //
            config.LoggingRules.Add(new LoggingRule("tests.*", LogLevel.Debug, consoleTarget) { Final = true, });
            //
            config.LoggingRules.Add(new LoggingRule("*", LogLevel.Info, consoleTarget));
            config.LoggingRules.Add(new LoggingRule("*", LogLevel.Debug, fileTarget));
            LogManager.Configuration = config;

            var logger = LogManager.GetLogger("Main");
            logger.Info("=============== Starting =================");

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
        }

        [TearDown]
        public void RestartBrokers()
        {
            foreach(var broker in _stoppedBrokers)
                Vagrant("ssh -c 'sudo service kafka start' " + broker);
            _stoppedBrokers.Clear();
        }

        //public void TearDown()
        //{
        //    _router.Dispose();
        //}

        //[Test]
        //public void ConnectionActorTest()
        //{
        //    var topic = "autocreate.test." + _rnd.Next();
        //    var conn = new Transport(_seedAddresses);
        //    var cmd = new GetMetaForTopicCmd { Topic = topic};
        //    conn.Post(cmd);
        //    Thread.Sleep(1000);
        //}

        // If failed to connect, messages are errored

        // if topic does not exists, it is created and pub-sub works
        [Test]
        public async void TopicIsAutocreated()
        {
            var topic = "autocreate.test." + _rnd.Next();
            var publishedCount = 10;
            var broker = new Router(_seedAddresses);
            await broker.ConnectAsync();
            var lala = Encoding.UTF8.GetBytes("la-la-la");
            // TODO: set wait to 5sec
            var requestTimeout = TimeSpan.FromSeconds(1000);

            var topics = broker.GetTopics().Result.Select(t => t.TopicName);
            Console.WriteLine("Topics: {0}", string.Join(", ", topics.Take(3)));

            var confirmedSent = new Subject<Message>();
            var publisher = new Publisher(topic) {
                OnTempError = tmp => _log.Info("Delayed {0} messages", tmp.Length),
                OnPermError = (ex, err) => _log.Info("Failed {0} messages: {1}", err.Length, ex.Message),
                OnShutdownDirty = shutdown => _log.Info("Failed {0} messages", shutdown.Length),
                OnSuccess = success => { 
                    success.ToList().ForEach(confirmedSent.OnNext);
                    _log.Info("Published message '{0}'", string.Join("; ", success.Select(m => Encoding.UTF8.GetString(m.Value)).ToArray()));
                }
            };
            publisher.Connect(broker);
            
            // start listening
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, maxWaitTimeMs: 1000, minBytesPerFetch: 1));
            await consumer.ConnectAsync();
            var received = new Subject<ReceivedMessage>();
            var receivedTxt = new List<string>();
            var consumerSubscription = consumer.
                Do(m => _log.Info("Got message")).
                Subscribe(received.OnNext);
            
            received.
                Select(m => Encoding.UTF8.GetString(m.Value)).
                Do(m => _log.Info("Received {0}", m)).
                Do(receivedTxt.Add).
                Subscribe();

            // start publishing
            var pubTask = Observable.Interval(TimeSpan.FromSeconds(1)).Take(publishedCount).
                Do(_ => publisher.Send(new Message { Value = lala }));

            _log.Info("Waiting for publisher to complete...");
            pubTask.ToTask().Wait(publishedCount * 1000 + 3000);
            await publisher.Close();
            // wait for 3sec for messages to propagate to subscriber
            _log.Info("Waiting for subscriber to get all messages");
            received.Take(publishedCount).ToTask().Wait(3000);

            Assert.AreEqual(publishedCount, receivedTxt.Count, "Did not received all messages");
            Assert.IsTrue(receivedTxt.All(m => m == "la-la-la"), "Unexpected message content");

            consumerSubscription.Dispose();
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
        }

        [Test]
        public void MultithreadingSend()
        {
            // for 10 minutes, 15 threads, each to its own topic, 100ms interval, send messages.
            // In addition, 5 topics are produced by 
            // Consumer must validate that messages were sent in order and all were received
        }

        [Test]
        public void Miltithreading2()
        {
            // subjects are produced by 15, 7, 3, 3, 2, 2, 2, 2, producers/threads
            // Consumer must validate that messages were sent in order and all were received
        }

        // consumer sees the same messages as publisher

        // if leader goes down, messages keep being accepted 
        // and are committed (can be read) within (5sec?)
        // Also, order of messages is preserved
        [Test]
        public async void LeaderDownRecovery()
        {
            var broker = new Router(_seedAddresses);
            await broker.ConnectAsync();

            var topic = "part33";
            if(!broker.GetAllTopics().Contains(topic))
                Vagrant(_part33CreateScript);

            var publisher = new Publisher(topic) { OnSuccess = msgs => _log.Debug("Sent {0} messages", msgs.Length) };
            publisher.Connect(broker);

            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic));
            await consumer.ConnectAsync();

            var postCount = 100;
            var postCount2 = 50;

            // read messages
            var received = new List<ReceivedMessage>();
            var receivedEvents = new ReplaySubject<ReceivedMessage>();
            var consumerSubscription = consumer.
                Synchronize().
                Subscribe(msg =>
                {
                    received.Add(msg);
                    receivedEvents.OnNext(msg);
                    _log.Debug("Received {0}/{1}", Encoding.UTF8.GetString(msg.Value), received.Count);
                });

            // send
            Observable.Interval(TimeSpan.FromMilliseconds(200)).
                Take(postCount).
                Subscribe(
                    i => publisher.Send(new Message { Value = Encoding.UTF8.GetBytes("msg " + i) }),
                    () => Console.WriteLine("Publisher complete")
                );

            // wait for first 50 messages to arrive
            await receivedEvents.Take(postCount).Count().ToTask();
            Assert.AreEqual(postCount, received.Count);

            // stop broker1. As messages have null-key, some of 50 of them have to end up on relocated broker1
            Vagrant("ssh -c 'sudo service kafka stop' broker1");
            _stoppedBrokers.Add("broker1");
            _log.Info("Stopped broker1");

            // post another 50 messages
            var sender2 = Observable.Interval(TimeSpan.FromMilliseconds(100)).
                Take(postCount2);

            sender2.Subscribe(
                    i => {
                        publisher.Send(new Message { Value = Encoding.UTF8.GetBytes("msg #2 " + i) });
                        _log.Debug("Sent msg #2 {0}", i);
                    },
                    () => Console.WriteLine("Publisher #2 complete")
                );

            _log.Debug("Waiting for #2 sender to complete");
            await sender2.ToTask();
            _log.Debug("Waiting 4sec for remaining messages");
            // TODO: when test is marked as async void, this await cause nunit to hang!!!?
            await Task.Delay(TimeSpan.FromSeconds(4)); // if unexpected messages arrive, let them in to detect failure
            //await receivedEvents.Take(postCount + postCount2).ToTask();
            Assert.AreEqual(postCount + postCount2, received.Count);
            consumerSubscription.Dispose();
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));

            _log.Debug("Done");
        }

        #if DEBUG
        [Test]
        public async void ListenerRecoveryTest()
        {
            var count = 400;
            var topic = "part3";
            var broker = new Router(_seedAddresses);
            _log.Debug("Connecting");
            await broker.ConnectAsync();

            _log.Debug("Filling out {0}", topic);
            var producer = new Publisher(topic);
            producer.Connect(broker);
            Enumerable.Range(1, count).
                Select(i => new Message() {Value = BitConverter.GetBytes(i)}).
                ForEach(producer.Send);
            await producer.Close();

            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, ConsumerStartLocation.TopicHead, maxBytesPerFetch: 4*8));
            await consumer.ConnectAsync();
            var current =0;
            var received = new ReplaySubject<ReceivedMessage>();
            var consumerSubscription = consumer.
                Subscribe(msg => {
                    current++;
                    if (current == 10)
                    {
                        var brokerMeta = broker.TestGetBrokerForPartition(consumer.Topic, msg.Partition);
                        var brokerName = GetBrokerNameFromIp(brokerMeta.Host);
                        _log.Info("Closing {0}", brokerName);
                        Vagrant("ssh -c 'sudo service kafka stop' "+brokerName);
                        _stoppedBrokers.Add(brokerName);
                    }

                    received.OnNext(msg);
                    _log.Info("Got: {0}", BitConverter.ToInt32(msg.Value, 0));
                });

            _log.Debug("Waiting for receiver complete");
            var count2 = await received.Take(count).Count().ToTask();
            Assert.AreEqual(count, count2);

            consumerSubscription.Dispose();
            await consumer.CloseAsync(TimeSpan.FromSeconds(5));
        }
#endif

        [Test]
        public async void CleanShutdownTest()
        {
            var broker = new Router(_seedAddresses);
            await broker.ConnectAsync();
            const string topic = "shutdown.test";

            var producer = new Publisher(topic) {
                OnTempError = tmpErrored => { },
                OnPermError = (e, messages) => Console.WriteLine("Publisher error: {0}", e.Message),
                OnShutdownDirty = dirtyShutdown => Console.WriteLine("Dirty shutdown"), 
                OnSuccess = success => { },
                BatchTime = TimeSpan.FromSeconds(2)
            };
            producer.Connect(broker);

            // set producer long batching period, 20 sec
            producer.BatchTime = TimeSpan.FromSeconds(20);
            producer.BatchSize = int.MaxValue;

            // start listener at the end of queue and accumulate received messages
            var received = new HashSet<string>();
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, maxWaitTimeMs: 30 * 1000));
            await consumer.ConnectAsync();
            var consumerSubscription = consumer
                                        .Select(msg => Encoding.UTF8.GetString(msg.Value))
                                        .Subscribe(m => received.Add(m));

            // send data, 5 msg/sec
            var sent = new HashSet<string>();
            var producerSubscription = Observable.Interval(TimeSpan.FromSeconds(1.0 / 5)).
                Select(i => string.Format("msg {0} {1}", i, Guid.NewGuid())).
                Do(m => sent.Add(m)).
                Select(msg => new Message
                {
                    Value = Encoding.UTF8.GetBytes(msg)
                }).
                Subscribe(producer.Send);

            // shutdown producer in 5 sec
            await Task.Delay(5000);
            producerSubscription.Dispose();
            await producer.Close();

            await broker.Close(TimeSpan.FromSeconds(4));

            // how to make sure nothing is sent after shutdown? listen to logger?  have connection events?

            // wait for 1sec for receiver to get all the messages
            await Task.Delay(1000);
            consumerSubscription.Dispose();
            await consumer.CloseAsync(TimeSpan.FromSeconds(4));

            // assert we received all the messages

            Assert.AreEqual(sent.Count, received.Count, string.Format("Sent and Receved size differs. Sent: {0} Recevied: {1}", sent.Count, received.Count));
            // compare sets and not lists, because of 2 partitions, send order and receive orser are not the same
            Assert.True(received.SetEquals(sent), "Sent and Received set differs");
        }

        [Test]
        public void DirtyShutdownTest()
        {

        }

        [Test]
        public async void KeyedMessagesPreserveOrder()
        {
            var routerProducer = new Router(_seedAddresses);
            await routerProducer.ConnectAsync();
            
            // create a topic with 3 partitions
            // TODO: how to skip initialization if topic already exists? Add router.GetAllTopics().Any(...)??? Or make it part of provisioning script?
            if(!routerProducer.GetAllTopics().Any(t => t == "part33"))
            {
                Vagrant(_part33CreateScript);
            }
            
            // create listener in a separate connection/broker
            var receivedMsgs = new List<ReceivedMessage>();
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, "part33"));
            await consumer.ConnectAsync();
            consumer.Synchronize().Subscribe(msg =>
            {
                lock (receivedMsgs)
                {
                    receivedMsgs.Add(msg);
                    //_log.Info("Received '{0}'/{1}/{2}", Encoding.UTF8.GetString(msg.Value), msg.Partition, BitConverter.ToInt32(msg.Key, 0));
                }
            });

            // sender is configured with 50ms batch period
            var producer = new Publisher("part33") { BatchTime = TimeSpan.FromMilliseconds(50)};
            producer.Connect(routerProducer);

            //
            // generate messages with 100ms interval in 10 threads
            //
            var sentMsgs = new List<Message>();
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
            Thread.Sleep(100*1000);
            senders.ForEach(s => s.Dispose());
            producer.Close().Wait();

            // wait for 3 sec for listener to catch up
            Thread.Sleep(3*1000);

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
            var notInOrder = Enumerable.Zip(
                keysSent.OrderBy(k => k.Key),
                keysReceived.OrderBy(k => k.Key),
                (s, r) => new { s, r, ok = s.Msgs.SequenceEqual(r.Msgs) }
            ).Where(_ => !_.ok).ToArray();

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
            var router = new Router(_seedAddresses);
            await router.ConnectAsync();

            // create new topic with 3 partitions
            if(!router.GetAllTopics().Contains("part33"))
                Vagrant(_part33CreateScript);

            // fill it out with 10K messages
            var producer = new Publisher("part33");
            producer.Connect(router);
            _log.Info("Sending data");
            Enumerable.Range(1, 10 * 1000).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            _log.Debug("Closing producer");
            await producer.Close();

            // consume tail-300 for each partition
            var offsets = (await router.GetPartitionsInfo("part33")).ToDictionary(p => p.Partition);
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, "part33", partitionOffsetProvider: p => offsets[p].Tail - 300));
            await consumer.ConnectAsync();
            var messages = consumer.
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
            var router = new Router(_seedAddresses);
            await router.ConnectAsync();
            var count = 100;

            var topic = "part32." + _rnd.Next();
            await router.ConnectAsync();
            _log.Info("Creating '{0}'", topic);
            Vagrant("ssh -c '/opt/kafka_2.10-0.8.1.1/bin/kafka-topics.sh --create --topic " + topic + " --partitions 3 --replication-factor 2 --zookeeper 192.168.56.2' broker1");

            // fill it out with 100 messages
            var producer = new Publisher(topic);
            producer.Connect(router);
            _log.Info("Sending data");
            Enumerable.Range(1, count).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(producer.Send);

            _log.Debug("Closing producer");
            await producer.Close();

            // read starting from the head
            var consumer = new Consumer(new ConsumerConfiguration(_seedAddresses, topic, ConsumerStartLocation.TopicHead));
            await consumer.ConnectAsync();
            var count2 = consumer.TakeUntil(DateTimeOffset.Now.AddSeconds(5))
                //.Do(val=>_log.Info("received value {0}", BitConverter.ToInt32(val.Value,0)))
                .Count().ToTask();
            Assert.AreEqual(count, await count2);
        }

        // if attempt to fetch from offset out of range, excption is thrown
        [Test]
        public void OutOfRangeOffsetThrows()
        {

        }

        // implicit offset is defaulted to fetching from the end
        [Test]
        public void DefaultPositionToTheTail()
        {

        }

        // Create a new 1-partition topic and sent 100 messages.
        // Read offsets, they should be [0, 100]
        [Test, Timeout(30*1000)]
        public async void ReadOffsets()
        {
            var router = new Router(_seedAddresses);
            var sentEvents = new Subject<Message>();
            var topic = "part12." + _rnd.Next();
            await router.ConnectAsync();
            _log.Info("Creating '{0}'", topic);
            Vagrant("ssh -c '/opt/kafka_2.10-0.8.1.1/bin/kafka-topics.sh --create --topic "+topic+" --partitions 1 --replication-factor 2 --zookeeper 192.168.56.2' broker1");

            // read offsets of empty queue
            var parts = await router.GetPartitionsInfo(topic);
            Assert.AreEqual(1, parts.Length, "Expected just one partition");
            Assert.AreEqual(-1L, parts[0].Head, "Expected start at 0");
            Assert.AreEqual(0L, parts[0].Tail, "Expected end at 100");

            var publisher = new Publisher(topic) { OnSuccess = e => e.ForEach(sentEvents.OnNext)};
            publisher.Connect(router);

            // send 100 messages
            Enumerable.Range(1, 100).
                Select(i => new Message { Value = BitConverter.GetBytes(i) }).
                ForEach(publisher.Send);
            _log.Info("Waiting for 100 sent messages");
            sentEvents.Subscribe(msg => _log.Debug("Sent {0}", BitConverter.ToInt32(msg.Value, 0)));
            await sentEvents.Take(100).ToTask();
            _log.Info("Closing publisher");
            await publisher.Close();

            // re-read offsets after messages published
            parts = await router.GetPartitionsInfo(topic);

            Assert.AreEqual(1, parts.Length, "Expected just one partition");
            Assert.AreEqual(0, parts[0].Partition, "Expected the only partition with Id=0");
            Assert.AreEqual(0L, parts[0].Head, "Expected start at 0");
            Assert.AreEqual(100L, parts[0].Tail, "Expected end at 100");
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

        static void Vagrant(string script)
        {
            // TODO: implement vagrant-control 
            var dir = AppDomain.CurrentDomain.BaseDirectory;
            dir = Path.Combine(dir, @"..\..\..\vagrant");
            dir = Path.GetFullPath(dir);
            var pi = new ProcessStartInfo(@"vagrant.exe", script)
            {
                CreateNoWindow = true,
                RedirectStandardError = true,
                RedirectStandardOutput = true,
                UseShellExecute = false,
                WorkingDirectory = dir
            };

            var p = Process.Start(pi);
            p.OutputDataReceived += (sender, args) => Console.WriteLine(args.Data);
            p.ErrorDataReceived += (sender, args) => Console.WriteLine(args.Data);
            p.BeginOutputReadLine();
            p.BeginErrorReadLine();
            p.WaitForExit();
        }

        private string GetBrokerNameFromIp(string ip)
        {
            switch(ip)
            {
                case "192.168.56.10": return "broker1";
                case "192.168.56.20": return "broker2";
                case "192.168.56.30": return "broker3";
                default: throw new Exception("Unknown ip: "+ip);
            }
        }

    }
}
