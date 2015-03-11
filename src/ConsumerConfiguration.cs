using System;
using kafka4net.ConsumerImpl;

namespace kafka4net
{
    /// <summary>
    /// Where to begin consuming. 
    ///   - TopicHead starts at the first message in each partition (beginning of the queue). 
    ///   - TopicTail starts at the most recent message of each partition (end of the queue)
    ///   - SpecifiedLocations starts at the locations specified by the PartitionOffsetProvider function (see constructor paramters).
    /// </summary>
    public enum ConsumerLocation : long
    {
        TopicStart = -2L,
        TopicEnd = -1L,
        SpecifiedLocations
    }

    /// <summary>
    /// Interface to configuration to describe to the consumer where to start consuming (and which partitions to consume from)
    /// </summary>
    public interface IStartPositionProvider
    {
        /// <summary>
        /// Called to determine which partitions to consume from. Return true to consume from it or false not to. 
        /// If this method returns true, it is expected that GetStartOffset will return a valid offset.
        /// </summary>
        /// <param name="partitionId">The partition ID to check if we should consume from</param>
        /// <returns>true to subscribe and consume from this partition, false to skip it</returns>
        bool ShouldConsumePartition(int partitionId);

        /// <summary>
        /// Gets the starting offset to use to consume at for this partition.
        /// </summary>
        /// <param name="partitionId">The partition ID</param>
        /// <returns>The next offset to consume. This will be the next offset consumed.</returns>
        long GetStartOffset(int partitionId);

        /// <summary>
        /// The logical start location represented by this IStartPositionProvider. 
        /// If this returns one of TopicHead or TopicTail, then all partitions returned from ShouldConsumePartition 
        /// Will be started from their current head or tail offset, and the GetStartOffset method will not be called.
        /// </summary>
        ConsumerLocation StartLocation { get; }
    }
    public interface IStopPositionProvider
    {
        /// <summary>
        /// Determines whether the consumer is finished consuming from a given partition based on the current message.
        /// Once this function returns false, the current message will be delivered but no further messages will be consumed from this partition.
        /// </summary>
        /// <param name="currentMessage"></param>
        /// <returns></returns>
        bool IsPartitionConsumingComplete(ReceivedMessage currentMessage);
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
        /// <param name="startPosition"></param>
        /// <param name="maxWaitTimeMs"></param>
        /// <param name="minBytesPerFetch"></param>
        /// <param name="maxBytesPerFetch"></param>
        /// <param name="highWatermark"></param>
        /// <param name="useFlowControl">
        ///     If set to true, subscriber must call consumers Ack function to keep data flowing.
        ///     Is used to prevent out of memory errors when subscriber is slow and data velocity is high 
        ///     (re-reading the log from beginning for example).
        ///     Make sure that subscriber consumes more than lowWatermark, or data might stop flowing because driver would
        ///     wait for subscriber to drain until lowWatermark and subscriber would wait for more data until continue processing 
        ///     (could happen when buffering is used).
        /// </param>
        /// <param name="lowWatermark"></param>
        /// <param name="stopPosition"></param>
        public ConsumerConfiguration(
            string seedBrokers,
            string topic, 
            IStartPositionProvider startPosition, 
            int maxWaitTimeMs=500, 
            int minBytesPerFetch = 1, 
            int maxBytesPerFetch=256*1024,
            int lowWatermark = 500,
            int highWatermark = 2000,
            bool useFlowControl = false,
            IStopPositionProvider stopPosition = null)
        {
            LowWatermark = lowWatermark;
            HighWatermark = highWatermark;
            UseFlowControl = useFlowControl;

            if(lowWatermark < 0)
                throw new ArgumentException("Can not be negative", "lowWatermark");

            if (highWatermark < 0)
                throw new ArgumentException("Can not be negative", "highWatermark");

            if(highWatermark < lowWatermark)
                throw new InvalidOperationException("highWatermark must be greater than lowWatermark");

            SeedBrokers = seedBrokers;
            StartPosition = startPosition;
            Topic = topic;
            MaxWaitTimeMs = maxWaitTimeMs;
            MinBytesPerFetch = minBytesPerFetch;
            MaxBytesPerFetch = maxBytesPerFetch;
            StopPosition = stopPosition ?? new StopPositionNever();
        }

        public string SeedBrokers { get; private set; }
        public IStartPositionProvider StartPosition { get; private set; }
        public IStopPositionProvider StopPosition { get; private set; }
        public string Topic { get; private set; }
        public int MaxWaitTimeMs  { get; private set; }
        public int MinBytesPerFetch { get; private set; }
        public int MaxBytesPerFetch { get; private set; }
        public readonly int LowWatermark;
        public readonly int HighWatermark;
        public readonly bool UseFlowControl;

    }
}
