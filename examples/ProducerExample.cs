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
