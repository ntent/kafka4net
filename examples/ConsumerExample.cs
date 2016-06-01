using System;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using kafka4net;
using kafka4net.ConsumerImpl;

namespace examples
{
    public static class ConsumerExample
    {
        // Notice, unlike java driver, port is not mandatory and will resolve to default value 9092.
        // Explicit ports are allowed though, for example "kafka1:9092, kafka2:9093, kafka3:9094"
        readonly static string _connectionString = "kafka1, kafka2, kafka3";
        readonly static string _topic = "some.topic";

        /// <summary>Simplest consumer with termination example</summary>
        public static async Task TakeForOneMinute()
        {
            // If you want to consume all messages in the topic, use TopicPositionFactory.Start
            // TopicPositionFactory.End will start waiting for new messages starting from the moment of subscription
            var consumer = new Consumer(new ConsumerConfiguration(_connectionString, _topic, new StartPositionTopicEnd()));

            consumer.OnMessageArrived.Subscribe(msg => {
                // Perform your own deserialization here
                var text = Encoding.UTF8.GetString(msg.Value);
                Console.WriteLine($"Got message: '{text}' Partition: {msg.Partition} Offset: {msg.Offset} Lag: {msg.HighWaterMarkOffset - msg.Offset}");
            });

            // Connecting starts when subscribing to OnMessageArrived. If you need to know when connection is actually one, wait for IsConnected task completion
            await consumer.IsConnected;
            Console.WriteLine("Connected");

            // Consume for one minute
            await Task.Delay(TimeSpan.FromMinutes(1));

            await consumer.CloseAsync();
            Console.WriteLine("Closed");
        }

        public static async Task SubscribeToMultipleTopics()
        {
            var rx = new Regex("some\\.topic\\..+");
            var cluster = new Cluster(_connectionString);
            await cluster.ConnectAsync();
            var allTopics = await cluster.GetAllTopicsAsync();
            var wantedTopics = allTopics.Where(topic => rx.IsMatch(topic)).ToArray();

            var consumers = wantedTopics.Select(topic => new Consumer(new ConsumerConfiguration(_connectionString, topic, new StartPositionTopicEnd()))).ToArray();
            consumers.AsParallel().ForAll(consumer => consumer.OnMessageArrived.Subscribe(msg => {
                var text = Encoding.UTF8.GetString(msg.Value);
                Console.WriteLine($"Got message '{text}' from topic '{msg.Topic}' partition {msg.Partition} offset {msg.Offset}");
            }));

            await Task.Delay(TimeSpan.FromMinutes(1));

            await Task.WhenAll(consumers.Select(consumer => consumer.CloseAsync()));
        }

    }
}
