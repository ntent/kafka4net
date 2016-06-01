[![NuGet version](https://badge.fury.io/nu/kafka4net.svg)](https://badge.fury.io/nu/kafka4net)
kafka4net: kafka-0.8 client
=========

#### Install: 
```Install-Package kafka4net```

##Features:
* Event-driven architecture, all asynchronous
* Automatic following changes of Leader Partition in case of broker failure
* Connection sharing: one connection per kafka broker is used
* Flow control: slow consumer will suspend fetching and prevent memory exhausting
* Integration tests are part of the codebase. Use Vagrant to provision 1 zookeeper and 3 kafka virtual servers
* Use RxExtensions library to expose API and for internal implementation
* Support compression (gzip, lz4, snappy). Unit-tested to be interoperable with Java implementation

##Not implemented:
* Offset Fetch/Commit API
* Only protocol for 0.8 (aka v0) is implemented at the moment

## Documentation
* [Design](https://github.com/ntent-ad/kafka4net/wiki/Design)
* [Troubleshooting](https://github.com/ntent-ad/kafka4net/wiki/Troubleshooting)

## Usage
### Consumer

```C#
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
    }
}
```



### Consumer from multiple partitions

```C#
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
```

### Producer
```C#
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using kafka4net;

namespace examples
{
    public static class ProducerExample
    {
        // Notice, unlike java driver, port is not mandatory and will resolve to default value 9092.
        // Explicit ports are allowed though, for example "kafka1:9092, kafka2:9093, kafka3:9094"
        readonly static string _connectionString = "kafka1, kafka2, kafka3";
        readonly static string _topic = "some.topic";

        public async static Task Produce100RandomMessages()
        {
            var rnd = new Random();
            var randomNumbers = Enumerable.Range(1, 100).Select(_ => rnd.Next().ToString());
            var producer = new Producer(_connectionString, new ProducerConfiguration(_topic));

            // Technically not mandatory, but good idea to listen to possible errors and act accordingly to your application requirements
            producer.OnPermError += (exception, messages) => Console.WriteLine($"Failed to write {messages.Length} because of {exception.Message}");

            // When message is confirmed by kafka broker to be persisted, OnSuccess is called. Can be used if upstream requires acknowlegement for extra reliability.
            producer.OnSuccess += messages => Console.WriteLine($"Sent {messages.Length} messages");

            await producer.ConnectAsync();

            foreach(var str in randomNumbers)
            {
                // Message.Key is optional. If not set, then driver will partition messages at random. In this case, for sake of example, partition by 1st character of the string
                var key = BitConverter.GetBytes(str[0]);
                // Implement your own serialization here.
                var msg = new Message { Value = Encoding.UTF8.GetBytes(str),  Key = key};
                producer.Send(msg);
            }

            // Await for every producer to complete. Note, that calling CloseAsync is safe: all bessages currently in buffers will be awaited until flushed.
            await producer.CloseAsync(TimeSpan.FromMinutes(1));
        }
    }
}
```