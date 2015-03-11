using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka4net.ConsumerImpl
{
    public class StopPositionNever : IStopPositionProvider
    {
        public bool IsPartitionConsumingComplete(ReceivedMessage currentMessage)
        {
            return false;
        }
    }

    public class StartAndStopAtExplicitOffsets : IStartPositionProvider, IStopPositionProvider
    {
        private readonly TopicPartitionOffsets _startingOffsets;
        private readonly TopicPartitionOffsets _stoppingOffsets;

        public StartAndStopAtExplicitOffsets(Dictionary<int, long> startingOffsets, Dictionary<int, long> stoppingOffsets)
        {
            _startingOffsets = new TopicPartitionOffsets("__xxx__",startingOffsets); 
            _stoppingOffsets = new TopicPartitionOffsets("__xxx__",stoppingOffsets); 
        }
        public StartAndStopAtExplicitOffsets(IEnumerable<KeyValuePair<int, long>> startingOffsets, IEnumerable<KeyValuePair<int, long>> stoppingOffsets)
        {
            _startingOffsets = new TopicPartitionOffsets("__xxx__",startingOffsets); 
            _stoppingOffsets = new TopicPartitionOffsets("__xxx__",stoppingOffsets); 
        }
        public StartAndStopAtExplicitOffsets(IEnumerable<Tuple<int, long>> startingOffsets, IEnumerable<Tuple<int, long>> stoppingOffsets)
        {
            _startingOffsets = new TopicPartitionOffsets("__xxx__",startingOffsets); 
            _stoppingOffsets = new TopicPartitionOffsets("__xxx__",stoppingOffsets); 
        }
        public StartAndStopAtExplicitOffsets(TopicPartitionOffsets startingOffsets, TopicPartitionOffsets stoppingOffsets)
        {
            _startingOffsets = startingOffsets;
            _stoppingOffsets = stoppingOffsets;
        }

        public bool ShouldConsumePartition(int partitionId)
        {
            // we should only consume from this partition if we were told to by the start offsets and also if we are not stopping where we already are.
            return _startingOffsets.ShouldConsumePartition(partitionId) && _startingOffsets.NextOffset(partitionId) != _stoppingOffsets.NextOffset(partitionId);
        }

        public long GetStartOffset(int partitionId)
        {
            return _startingOffsets.GetStartOffset(partitionId);
        }

        public ConsumerLocation StartLocation
        {
            get { return ConsumerLocation.SpecifiedLocations; }
        }

        public bool IsPartitionConsumingComplete(ReceivedMessage currentMessage)
        {
            return _stoppingOffsets.IsPartitionConsumingComplete(currentMessage);
        }
    }

    public class StartPositionTopicEnd : StartPositionTopicTime
    {
        public override ConsumerLocation StartLocation { get { return ConsumerLocation.TopicEnd; } }
    }

    public class StartPositionTopicStart : StartPositionTopicTime
    {
        public override ConsumerLocation StartLocation { get { return ConsumerLocation.TopicStart; } }
    }

    public abstract class StartPositionTopicTime : IStartPositionProvider
    {
        private readonly ISet<int> _partitionsToConsume;

        public StartPositionTopicTime() : this(null) { }
        public StartPositionTopicTime(IEnumerable<int> partitionsToConsume) : this(new HashSet<int>(partitionsToConsume)) {}
        public StartPositionTopicTime(ISet<int> partitionsToConsume)
        {
            _partitionsToConsume = partitionsToConsume;
        }

        public bool ShouldConsumePartition(int partitionId) { return _partitionsToConsume == null || _partitionsToConsume.Contains(partitionId); }
        public long GetStartOffset(int partitionId) { throw new NotImplementedException(); }
        public abstract ConsumerLocation StartLocation { get; }
    }
}
