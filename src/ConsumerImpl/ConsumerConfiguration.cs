using System;

namespace kafka4net.ConsumerImpl
{
    /// <summary>
    /// Where to begin consuming. 
    ///   - TopicHead starts at the first message in each partition (beginning of the queue). 
    ///   - TopicTail starts at the most recent message of each partition (end of the queue)
    ///   - SpecifiedLocations starts at the locations specified by the PartitionOffsetProvider function (see constructor paramters).
    /// </summary>
    public enum ConsumerStartLocation : long
    {
        TopicHead = -2L,
        TopicTail = -1L,
        SpecifiedLocations
    }

    public class ConsumerConfiguration
    {
        /// <summary>
        /// Subscription is performed asynchronously.
        /// </summary>
        /// <param name="seedBrokers">Comma separated list of seed brokers. Port numbers are optional.
        /// <example>192.168.56.10,192.168.56.20:8081,broker3.local.net:8181</example>
        /// </param>
        /// <param name="topic"></param>
        /// <param name="startLocation"></param>
        /// <param name="maxWaitTimeMs"></param>
        /// <param name="minBytesPerFetch"></param>
        /// <param name="maxBytesPerFetch"></param>
        /// <param name="partitionOffsetProvider">
        ///     If not provided or null, all partitions are positioned at the end (only new messages will be picked up)
        /// </param>
        public ConsumerConfiguration(
            string seedBrokers,
            string topic, 
            ConsumerStartLocation startLocation = ConsumerStartLocation.TopicTail, 
            int maxWaitTimeMs=500, 
            int minBytesPerFetch = 1, 
            int maxBytesPerFetch=256*1024,
            Func<int,long> partitionOffsetProvider = null)
        {
            if(startLocation==ConsumerStartLocation.SpecifiedLocations && partitionOffsetProvider == null)
                throw new ArgumentException("If StartLocation is ConsumerStartLocation.SpecifiedLocations, PartitionOffsetProvider must be set");

            SeedBrokers = seedBrokers;
            PartitionOffsetProvider = partitionOffsetProvider;
            StartLocation = startLocation;
            Topic = topic;
            MaxWaitTimeMs = maxWaitTimeMs;
            MinBytesPerFetch = minBytesPerFetch;
            MaxBytesPerFetch = maxBytesPerFetch;
        }

        public string SeedBrokers { get; private set; }
        public ConsumerStartLocation StartLocation { get; private set; }
        public Func<int, long> PartitionOffsetProvider { get; private set; }
        public string Topic { get; private set; }
        public int MaxWaitTimeMs  { get; private set; }
        public int MinBytesPerFetch { get; private set; }
        public int MaxBytesPerFetch { get; private set; }

    }
}
