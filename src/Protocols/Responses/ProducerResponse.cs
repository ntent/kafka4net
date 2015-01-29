using System.Linq;

namespace kafka4net.Protocols.Responses
{
    class ProducerResponse
    {
        internal TopicResponse[] Topics;

        internal class TopicResponse
        {
            public string TopicName;
            public PartitionResponse[] Partitions;

            public override string ToString()
            {
                return string.Format("{0} [{1}]", TopicName, Partitions == null ? "null" : string.Join("\n  ", Partitions.AsEnumerable()));
            }
        }

        internal class PartitionResponse
        {
            public int Partition;
            public ErrorCode ErrorCode;
            public long Offset;

            public override string ToString()
            {
                return string.Format("Part: {0}, Err: {1} Offset: {2}", Partition, ErrorCode, Offset);
            }
        }

        public override string ToString()
        {
            return string.Format("Topics: [{0}]", Topics == null ? "null" : string.Join("\n ", Topics.AsEnumerable()));
        }
    }
}
