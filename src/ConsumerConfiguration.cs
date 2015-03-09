using System;

namespace kafka4net
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
        ///     If not provided or null, all partitions are positioned at the end  (only new messages will be picked up)
        /// </param>
        /// <param name="useFlowControl">
        ///     If set to true, subscriber must call consumers Ack function to keep data flowing.
        ///     Is used to prevent out of memory errors when subscriber is slow and data velocity is high 
        ///     (re-reading the log from beginning for example).
        ///     Make sure that subscriber consumes more than lowWatermark, or data might stop flowing because driver would
        ///     wait for subscriber to drain until lowWatermark and subscriber would wait for more data until continue processing 
        ///     (could happen when buffering is used).
        /// </param>
        /// <param name="partitionStopConditionCheckFunc">A dynamic callback that will determine whether to stop reading from a partition. Used to control the end time for consuming.</param>
        public ConsumerConfiguration(
            string seedBrokers,
            string topic, 
            ConsumerStartLocation startLocation = ConsumerStartLocation.TopicTail, 
            int maxWaitTimeMs=500, 
            int minBytesPerFetch = 1, 
            int maxBytesPerFetch=256*1024,
            Func<int,long> partitionOffsetProvider = null,
            int lowWatermark = 500,
            int highWatermark = 2000,
            bool useFlowControl = false,
            Func<ReceivedMessage, bool> partitionStopConditionCheckFunc = null)
        {
            LowWatermark = lowWatermark;
            HighWatermark = highWatermark;
            UseFlowControl = useFlowControl;

            if(startLocation==ConsumerStartLocation.SpecifiedLocations && partitionOffsetProvider == null)
                throw new ArgumentException("If StartLocation is ConsumerStartLocation.SpecifiedLocations, PartitionOffsetProvider must be set");

            if (startLocation != ConsumerStartLocation.SpecifiedLocations && partitionOffsetProvider != null)
                throw new ArgumentException("If StartLocation is NOT ConsumerStartLocation.SpecifiedLocations then PartitionOffsetProvider must NOT be set");

            if(lowWatermark < 0)
                throw new ArgumentException("Can not be negative", "lowWatermark");

            if (highWatermark < 0)
                throw new ArgumentException("Can not be negative", "highWatermark");

            if(highWatermark < lowWatermark)
                throw new InvalidOperationException("highWatermark must be greater than lowWatermark");

            SeedBrokers = seedBrokers;
            PartitionOffsetProvider = partitionOffsetProvider;
            StartLocation = startLocation;
            Topic = topic;
            MaxWaitTimeMs = maxWaitTimeMs;
            MinBytesPerFetch = minBytesPerFetch;
            MaxBytesPerFetch = maxBytesPerFetch;
            PartitionStopConditionCheckFunc = partitionStopConditionCheckFunc;
        }

        public string SeedBrokers { get; private set; }
        public ConsumerStartLocation StartLocation { get; private set; }
        public Func<int, long> PartitionOffsetProvider { get; private set; }
        public string Topic { get; private set; }
        public int MaxWaitTimeMs  { get; private set; }
        public int MinBytesPerFetch { get; private set; }
        public int MaxBytesPerFetch { get; private set; }
        public readonly int LowWatermark;
        public readonly int HighWatermark;
        public readonly bool UseFlowControl;
        public Func<ReceivedMessage, bool> PartitionStopConditionCheckFunc { get; private set; } 

    }
}
