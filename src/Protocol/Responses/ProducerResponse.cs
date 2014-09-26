using System.Collections;
using System.Linq;

namespace kafka4net.Protocol.Responses
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
                return string.Format("Topic: {0} [{1}]", TopicName, string.Join(", ", Partitions.AsEnumerable()));
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
            return string.Format("[{0}]", string.Join(", ", Topics.AsEnumerable()));
        }
    }
}
