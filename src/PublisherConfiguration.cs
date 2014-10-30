using System;
using System.Threading;

namespace kafka4net
{
    public class PublisherConfiguration
    {
        /// <summary>
        /// Short constructor for PublisherConfiguration. Defaults used for not specified settings.
        /// Default values are:
        ///     RequiredAcks: 1
        ///     Timeout: 1s
        ///     BatchFlushTime: 500ms
        ///     BatchFlushSize: 1000
        /// </summary>
        /// <param name="seedBrokers">Comma separated list of seed brokers. Port numbers are optional.
        /// <example>192.168.56.10,192.168.56.20:8081,broker3.local.net:8181</example>
        /// </param>
        /// <param name="topic"></param>
        public PublisherConfiguration(
            string seedBrokers,
            string topic)
        {
            SeedBrokers = seedBrokers;
            Topic = topic;
            RequiredAcks = 1;
            BatchFlushSize = 1000;
            BatchFlushTime = TimeSpan.FromMilliseconds(500);
            ProduceRequestTimeout = TimeSpan.FromSeconds(1);
        }

        /// <summary>
        /// Full constructor for PublisherConfiguration. Specify all settings.
        /// Default values are:
        ///     RequiredAcks: 1
        ///     Timeout: 1s
        ///     BatchFlushTime: 500ms
        ///     BatchFlushSize: 1000 
        /// </summary>
        /// <param name="seedBrokers"></param>
        /// <param name="topic"></param>
        /// <param name="requiredAcks">Number of Kafka servers required to acknowledge the write for it to be considered successful. Default 1.</param>
        /// <param name="batchFlushTime">Max time to wait before flushing messages. Default 500ms.</param>
        /// <param name="batchFlushSize">Max number of messages to accumulate before flushing messages. Default 1000.</param>
        public PublisherConfiguration(
            string seedBrokers,
            string topic,
            TimeSpan batchFlushTime,
            int batchFlushSize = 1000,
            short requiredAcks = 1)
            : this(seedBrokers, topic)
        {
            BatchFlushTime = batchFlushTime;
            BatchFlushSize = batchFlushSize;
            RequiredAcks = requiredAcks;
        }

        public string SeedBrokers { get; private set; }
        public string Topic { get; private set; }
        public short RequiredAcks { get; private set; }
        public TimeSpan BatchFlushTime { get; private set; }
        public int BatchFlushSize { get; private set; }
        public TimeSpan ProduceRequestTimeout { get; private set; }
        public int ProduceRequestTimeoutMs { get { return (int)Math.Floor(ProduceRequestTimeout.TotalMilliseconds); } }

    }
}
