using System;

namespace kafka4net.Internal
{
    internal class PartitionStateChangeEvent
    {
        public PartitionStateChangeEvent(string topic, int partitionId, ErrorCode errorCode)
        {
            Topic = topic;
            PartitionId = partitionId;
            ErrorCode = errorCode;
        }

        public readonly string Topic;
        public readonly int PartitionId;
        public readonly ErrorCode ErrorCode;
    }
}
