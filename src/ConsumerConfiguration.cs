using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
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
        internal string SeedBrokers { get; private set; }
        internal IStartPositionProvider StartPosition { get; private set; }
        internal IStopPositionProvider StopPosition { get; private set; }
        internal string Topic { get; private set; }
        internal int MaxWaitTimeMs { get; private set; }
        internal int MinBytesPerFetch { get; private set; }
        internal int MaxBytesPerFetch { get; private set; }
        internal bool Autoconnect = true;

        private ITargetBlock<ReceivedMessage> _action;

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
        /// <param name="stopPosition"></param>
        private ConsumerConfiguration(
            string seedBrokers,
            string topic, 
            IStartPositionProvider startPosition, 
            int maxWaitTimeMs=500, 
            int minBytesPerFetch = 1, 
            int maxBytesPerFetch=256*1024,
            IStopPositionProvider stopPosition = null)
        {
            SeedBrokers = seedBrokers;
            StartPosition = startPosition;
            Topic = topic;
            MaxWaitTimeMs = maxWaitTimeMs;
            MinBytesPerFetch = minBytesPerFetch;
            MaxBytesPerFetch = maxBytesPerFetch;
            StopPosition = stopPosition ?? new StopPositionNever();
        }

        public ConsumerConfiguration(string seedBrokers, string topic) : this(seedBrokers, topic, new StartPositionTopicEnd())
        {
        }

        //
        // Builder
        //

        public ConsumerConfiguration WithMaxWaitTimeMs(int maxWaitTimeMs)
        {
            MaxWaitTimeMs = maxWaitTimeMs;
            return this;
        }

        public ConsumerConfiguration WithMinBytesPerFetch(int minBytesPerFetch)
        {
            MinBytesPerFetch = minBytesPerFetch;
            return this;
        }

        public ConsumerConfiguration WithMaxBytesPerFetch(int maxBytesPErFetch)
        {
            MaxBytesPerFetch = maxBytesPErFetch;
            return this;
        }

        public ConsumerConfiguration WithStartPositionAtBeginning()
        {
            StartPosition = new StartPositionTopicStart();
            return this;
        }

        public ConsumerConfiguration WithStartPosition(IStartPositionProvider startPositionProvider)
        {
            StartPosition = startPositionProvider;
            return this;
        }

        public ConsumerConfiguration WithStopPosition(IStopPositionProvider stop)
        {
            StopPosition = stop;
            return this;
        }

        public ConsumerConfiguration WithAction(Action<ReceivedMessage> action, int buffer = 1000, int degreeOfParallelism = 1)
        {
            _action = new ActionBlock<ReceivedMessage>(action, new ExecutionDataflowBlockOptions { BoundedCapacity = buffer, MaxDegreeOfParallelism = degreeOfParallelism });
            return this;
        }

        public ConsumerConfiguration WithAction(Func<ReceivedMessage,Task> action, int buffer = 1000, int degreeOfParallelism = 1)
        {
            _action = new ActionBlock<ReceivedMessage>(action, new ExecutionDataflowBlockOptions { BoundedCapacity = buffer, MaxDegreeOfParallelism = degreeOfParallelism });
            return this;
        }

        /// <summary>
        /// Make sure to set BoundedCapacity of your block, otherwise you will get OutOfMemory exception if topic contains many messages and handler is not fast enough
        /// </summary>
        public ConsumerConfiguration WithAction(ITargetBlock<ReceivedMessage> action)
        {
            if (action == null)
                throw new ArgumentNullException("action");

            _action = action;
            return this;
        }

        public ConsumerConfiguration WithAutoconnect(bool autoconnect)
        {
            Autoconnect = autoconnect;
            return this;
        }


        public Consumer Build()
        {
            if(_action == null)
                throw new InvalidOperationException("Action have not been configured");
            return new Consumer(this, _action);
        }
    }
}
