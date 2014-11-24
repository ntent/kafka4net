using System.Globalization;
using System.Linq;

namespace kafka4net.Protocols.Responses
{
    class OffsetResponse
    {
        public string TopicName;
        public PartitionOffsetData[] Partitions;

        public class PartitionOffsetData
        {
            public int Partition;
            public ErrorCode ErrorCode;
            public long[] Offsets;

            public override string ToString()
            {
                return string.Format("{0}:{1}:[{2}]", Partition, ErrorCode,
                    string.Join(",", Offsets.Select(o => o.ToString(CultureInfo.InvariantCulture))));
            }
        }

        public override string ToString()
        {
            return string.Format("{0}:[{1}]", TopicName, string.Join(",", Partitions.Select(p => p.ToString())));
        }
    }
}
