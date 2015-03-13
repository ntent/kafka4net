using System;

namespace kafka4net
{
    class PartitionFailedException : Exception
    {
        public readonly string Topic;
        public readonly int Partition;
        public readonly ErrorCode ErrorCode;

        public PartitionFailedException(string topic, int partition, ErrorCode errorCode)
        {
            Topic = topic;
            Partition = partition;
            ErrorCode = errorCode;
        }

        public override string Message
        {
            get { return string.Format("Topic '{0}' partition {1} failed with error code {2}", Topic, Partition, ErrorCode); }
        }
    }
}
