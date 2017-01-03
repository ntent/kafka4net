using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Text;
using System.Threading.Tasks;
using kafka4net;
using log4net;
using System.Reactive.Threading.Tasks;
using System.Threading;

namespace examples
{
    public static class ProducerExample
    {
        // Notice, unlike java driver, port is not mandatory and will resolve to default value 9092.
        // Explicit ports are allowed though, for example "kafka1:9092, kafka2:9093, kafka3:9094"
        private static readonly string _connectionString = "kafka1, kafka2, kafka3";
        private static readonly string _topic = "some.topic";
        private static ILog _log = LogManager.GetLogger(typeof (ProducerExample));

        public static async Task Produce100RandomMessages()
        {
            var rnd = new Random();
            var randomNumbers = Enumerable.Range(1, 100).Select(_ => rnd.Next().ToString());
            var producer = new Producer(_connectionString, new ProducerConfiguration(_topic));

            // Technically not mandatory, but good idea to listen to possible errors and act accordingly to your application requirements
            producer.OnPermError +=
                (exception, messages) =>
                    Console.WriteLine($"Failed to write {messages.Length} because of {exception.Message}");

            // When message is confirmed by kafka broker to be persisted, OnSuccess is called. Can be used if upstream requires acknowlegement for extra reliability.
            producer.OnSuccess += messages => Console.WriteLine($"Sent {messages.Length} messages");

            await producer.ConnectAsync();

            foreach (var str in randomNumbers)
            {
                // Message.Key is optional. If not set, then driver will partition messages at random. In this case, for sake of example, partition by 1st character of the string
                var key = BitConverter.GetBytes(str[0]);
                // Implement your own serialization here.
                var msg = new Message {Value = Encoding.UTF8.GetBytes(str), Key = key};
                producer.Send(msg);
            }

            // Await for every producer to complete. Note, that calling CloseAsync is safe: all bessages currently in buffers will be awaited until flushed.
            await producer.CloseAsync(TimeSpan.FromMinutes(1));
        }

        public static async Task ProduceMessages(
            string brokers, 
            string topic, 
            int produceDelayMs, 
            int numMessages,
            string file, 
            CancellationToken cancel)
        {
            var producer = new Producer(brokers, new ProducerConfiguration(topic));

            // Technically not mandatory, but good idea to listen to possible errors and act accordingly to your application requirements
            producer.OnPermError +=
                (exception, messages) =>
                   _log.Error($"Failed to write {messages.Length} because of {exception.Message}");

            // When message is confirmed by kafka broker to be persisted, OnSuccess is called. Can be used if upstream requires acknowlegement for extra reliability.
            producer.OnSuccess +=
                messages => { foreach (var msg in messages) _log.Info($"Sent {Encoding.UTF8.GetString(msg.Value)}"); };

            _log.Info("Connecting...");
            await producer.ConnectAsync();

            _log.Info("Producing...");
            Func<long, string> dataFunc;
            if (file == null)
                dataFunc = i => i.ToString();
            else
            {
                var fileData = File.ReadAllLines(file);
                dataFunc = i => fileData[i%fileData.Length];
            }

            var data = Observable.Interval(TimeSpan.FromMilliseconds(produceDelayMs)).Select(dataFunc);
            if (numMessages > 0)
                data = data.Take(numMessages);

            var sendTask = data.Do(d => producer.Send(new Message {Value = Encoding.UTF8.GetBytes(d)})).ToTask(cancel);

            await sendTask;

            _log.Info("Closing...");
            await producer.CloseAsync(TimeSpan.FromSeconds(10));

        }
    }
}
