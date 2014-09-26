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
using kafka4net.Utils;
using NLog;
using NLog.Config;
using NLog.Targets;
using NUnit.Framework;
using NUnit.Framework.Constraints;
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

            consoleTarget.Layout = "${level} [${threadname}:${threadid}] ${logger} ${message} ${exception:format=tostring}";
            fileTarget.FileName = "${basedir}../../../../log.txt";
            fileTarget.Layout = "${longdate} ${level} [${threadname}:${threadid}] ${logger:shortName=true} ${message} ${exception:format=tostring,stacktrace:innerFormat=tostring,stacktrace}";

            // disable Transport noise
            //config.LoggingRules.Add(new LoggingRule("kafka4net.Protocol.Transport", LogLevel.Info, fileTarget) { Final = true});

            var rule1 = new LoggingRule("*", LogLevel.Info, consoleTarget);
            config.LoggingRules.Add(rule1);
            var rule2 = new LoggingRule("*", LogLevel.Debug, fileTarget);
            config.LoggingRules.Add(rule2);
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
                Vagrant("ssh -c 'sudo services start kafka' "+broker);
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

        // if topic does not exists, it is created within (1sec?)
        [Test]
        public void TopicIsAutocreated()
        {
            var topic = 
                //"part1";
                //"autocreate.test.230719751"; 
                "autocreate.test." + _rnd.Next();
            var broker = new Router(_seedAddresses);
            broker.ConnectAsync().Wait();
            var lala = Encoding.UTF8.GetBytes("la-la-la");
            // TODO: set wait to 5sec
            var requestTimeout = TimeSpan.FromSeconds(1000);

            var topics = broker.GetTopics().Result.Select(t => t.TopicName);
            Console.WriteLine("Topics: {0}", string.Join(", ", topics.Take(3)));

            var confirmedSent = new Subject<Message>();
            var publisher = new Publisher(topic) {
                OnTempError = tmp => Console.WriteLine("Delayed {0} messages", tmp.Length),
                OnPermError = (ex, err) => Console.WriteLine("Failed {0} messages: {1}", err.Length, ex.Message),
                OnShutdownDirty = shutdown => Console.WriteLine("Failed {0} messages", shutdown.Length),
                OnSuccess = success => { 
                    success.ToList().ForEach(confirmedSent.OnNext);
                    Console.WriteLine("Got message '{0}'", string.Join("; ",success.Select(m => Encoding.UTF8.GetString(m.Value)).ToArray()));
                }
            };
            publisher.Connect(broker);
            //var msg = new Message { Value = lala};
            //publisher.Send(msg);
            //Thread.Sleep(100*1000);
            
            // start listening
            var consumer = new Consumer(topic, broker, maxWaitTimeMs: 8000, minBytes: 1);
            var received = new Subject<ReceivedMessage>();
            var received2 = new List<ReceivedMessage>();
            var listener = consumer.AsObservable.Subscribe(received.OnNext);
            // start publishing
            var pubSub = Observable.Interval(TimeSpan.FromSeconds(1)).Take(10).
                Subscribe(_ => publisher.Send(new Message { Value = lala }));

            //var res = listener.Wait(requestTimeout);
            var tt = confirmedSent.Take(10);
            var tt2 = received.Do(received2.Add).Take(10);
            Assert.IsTrue(tt.ToTask().Wait(requestTimeout), "Timeout waiting for 10 message to be sent");
            Assert.IsTrue(tt2.ToTask().Wait(requestTimeout), "Timeout waiting for 10 message receive");
            Assert.AreEqual(10, received2.Count, "Should receive at least one message");
            Assert.IsTrue(received2.TrueForAll(m => m.Value.SequenceEqual(lala)), "Unexpected message content");

            // TODO: confirm that exactly 10 messages an no more has been received

            //consumer.Close();
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
        public void LeaderDownRecovery()
        {
            //var script = "./kafka-topics.sh  --create --topic part3 --partitions 1 --replication-factor 3 --zookeeper 192.168.56.2";
            //Vagrant(script);
            var broker = new Router(_seedAddresses);
            broker.ConnectAsync().Wait();
            var publisher = new Publisher("part3") { 
                OnTempError = messages => Console.WriteLine("Publisher temporary error. Meesages: {0}", messages.Length),
                OnPermError = (exception, messages) => Console.WriteLine("Perm error"),
                OnShutdownDirty = messages => Console.WriteLine("Close dirty"),
                OnSuccess = messages => { }
            };
            publisher.Connect(broker);
            var consumer = new Consumer("part3", broker);

            var postCount = 500;
            Observable.Interval(TimeSpan.FromMilliseconds(200)).
                Take(postCount).
                Subscribe(
                    i => 
                    { 
                        publisher.Send(new Message { Value = Encoding.UTF8.GetBytes("msg " + i) }); 
                        Debug.WriteLine("Sent msg {0}", i);
                    },
                    () => Console.WriteLine("Publisher complete")
                );

            // keep reading messages
            var received = new List<ReceivedMessage>();
            var receivedEvents = new ReplaySubject<ReceivedMessage>();
            consumer.AsObservable.Subscribe(msg =>
            {
                received.Add(msg);
                receivedEvents.OnNext(msg);
                Debug.WriteLine(string.Format("Received {0}", Encoding.UTF8.GetString(msg.Value)));
            });

            // wait for first 50 messages to arrive
            receivedEvents.Take(postCount).Count().ToTask().Wait();
            Assert.AreEqual(postCount, received.Count);

            // stop broker1
            // TODO:
            Console.WriteLine("Stop server");
            //Console.ReadLine();

            // post another 50 messages
            Observable.Interval(TimeSpan.FromMilliseconds(100)).
                Take(50).
                Subscribe(
                    i => publisher.Send(new Message { Value = Encoding.UTF8.GetBytes("msg #2 " + i) }),
                    () => Console.WriteLine("Publisher #2 complete")
                );

            // make sure that all 100 messages have been read within 10sec
            receivedEvents.Take(100).Count().ToTask().Wait();
            Thread.Sleep(4000); // if unexpected messages arrive, let them in
            Assert.AreEqual(100, received.Count);
        }

        #if DEBUG
        [Test]
        public void ListenerRecoveryTest()
        {
            var broker = new Router(_seedAddresses);
            broker.ConnectAsync().Wait();
            var consumer = new Consumer("part3", broker, offset: -2L, maxBytes: 256);
            var current =0;
            var received = new ReplaySubject<ReceivedMessage>();
            consumer.AsObservable.
                Subscribe(msg => {
                    current++;
                    if (current == 10)
                    {
                        var brokerMeta = broker.TestGetBrokerForPartition(consumer.Topic, msg.Partition);
                        var brokerName = GetBrokerNameFromIp(brokerMeta.Host);
                        Console.WriteLine("Closing {0}", brokerName);
                        Vagrant("ssh -c 'sudo service kafka stop' "+brokerName);
                        _stoppedBrokers.Add(brokerName);
                    }

                    received.OnNext(msg);
                    Console.WriteLine("Got: {0}", Encoding.UTF8.GetString(msg.Value));
                });

            var count = received.Take(400).Count().ToTask().Result;
            Assert.AreEqual(400, count);
        }
#endif

        [Test]
        public async Task CleanShutdownTest()
        {
            var broker = new Router(_seedAddresses);
            var fetchBroker = new Router(_seedAddresses);
            await broker.ConnectAsync();
            await fetchBroker.ConnectAsync();
            const string topic = "shutdown.test";

            var producer = new Publisher(topic) {
                OnTempError = tmpErrored => { },
                OnPermError = (e, messages) => Console.WriteLine("Publisher error: {0}", e.Message),
                OnShutdownDirty = dirtyShutdown => Console.WriteLine("Dirty shutdown"), 
                OnSuccess = success => { },
                BatchTime = TimeSpan.FromSeconds(20)
            };
            producer.Connect(broker);

            // set producer long batching period, 20 sec
            producer.BatchTime = TimeSpan.FromSeconds(20);
            producer.BatchSize = int.MaxValue;

            // start listener at the end of queue and accumulate received messages
            var received = new HashSet<string>();
            var consumer = new Consumer(topic, fetchBroker, maxWaitTimeMs: 30 * 1000);
            consumer.AsObservable.Select(msg => Encoding.UTF8.GetString(msg.Value)).Subscribe(m => received.Add(m));

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
            var t1 = producer.Close();

            // await for producer for 2 sec
            Assert.IsTrue(t1.Wait(2000), "Timed out waiting for consumer");

            await broker.Close(TimeSpan.FromSeconds(4));
            fetchBroker.Close(TimeSpan.FromSeconds(20));    // fetch may timeout due to long pooling, dont wait for it

            // how to make sure nothing is sent after shutdown? listen to logger?  have connection events?

            // wait for 1sec for receiver to get all the messages
            await Task.Delay(1000);

            // assert we received all the messages
            //consumer.Close();

            Assert.AreEqual(sent.Count, received.Count, string.Format("Sent and Receved size differs. Sent: {0} Recevied: {1}", sent.Count, received.Count));
            // compare sets and not lists, because of 2 partitions, send order and receive orser are not the same
            Assert.True(received.SetEquals(sent), "Sent and Received set differs");
        }

        [Test]
        public void DirtyShutdownTest()
        {

        }

        [Test]
        public void KeyedMessagesPreserveOrder()
        {
            var routerProducer = new Router(_seedAddresses);
            var routerListener = new Router(_seedAddresses);
            Task.WaitAll(routerProducer.ConnectAsync(), routerListener.ConnectAsync());
            
            // create a topic with 3 partitions
            // TODO: how to skip initialization if topic already exists? Add router.GetAllTopics().Any(...)??? Or make it part of provisioning script?
            if(!routerProducer.GetAllTopics().Any(t => t == "part33"))
            {
                var script = "ssh -c '/opt/kafka_2.10-0.8.1.1/bin/kafka-topics.sh --create --topic part33 --partitions 3 --replication-factor 2 --zookeeper 192.168.56.2' broker1";
                Vagrant(script);
            }
            
            // create listener in a separate connection/broker
            var receivedMsgs = new List<ReceivedMessage>();
            var consumer = new Consumer("part33", routerListener);
            consumer.AsObservable.Subscribe(msg =>
            {
                lock (receivedMsgs)
                {
                    receivedMsgs.Add(msg);
                }
            });

            // sender is configured with 50ms batch period
            var producer = new Publisher("part33") { BatchTime = TimeSpan.FromMilliseconds(50)};
            producer.Connect(routerProducer);

            //
            // generate messages with 100ms interval in 10 threads
            //
            var sentMsgs = new List<Message>();
            var senders = Enumerable.Range(1, 10).
                Select(thread => Observable.
                    Interval(TimeSpan.FromMilliseconds(10)).
                    Select(i => {
                        var str = "msg " + i + " " + Guid.NewGuid();
                        var bin = Encoding.UTF8.GetBytes(str);
                        var msg = new Message
                        {
                            Key = BitConverter.GetBytes((int)i % 10),
                            Value = bin
                        };
                        lock (sentMsgs)
                        {
                            sentMsgs.Add(msg);
                        }
                        return msg;
                    }).
                    Subscribe(producer.Send)
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
            // group messages by partition and compare lists in each partition to be the same (order should be preserved within partition)
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
                notInOrder.ForEach(_ => _log.Error("Failed order in:\n{0}\n{1}", string.Join("|", _.s.Msgs.Take(5)), string.Join("|", _.r.Msgs.Take(5))));
            }
            Assert.IsTrue(!notInOrder.Any(), "Detected out of order messages");
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
            var pi = new ProcessStartInfo(@"C:\HashiCorp\Vagrant\bin\vagrant.exe", script)
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
