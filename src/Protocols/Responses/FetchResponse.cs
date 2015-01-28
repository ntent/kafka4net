using System.Linq;

namespace kafka4net.Protocols.Responses
{
    class FetchResponse
    {
        public TopicFetchData[] Topics;

        public class TopicFetchData
        {
            public string Topic;
            public PartitionFetchData[] Partitions;

            public override string ToString()
            {
                return ToString(false);
            }

            public string ToString(bool onlyPartitionsWithMessages)
            {
                return string.Format("'{0}' PartId:ErrorCode:HighWatermarkOffset:MessageCount [{1}]", Topic, Partitions == null ? "null" : string.Join("\n  ", Partitions.Where(p => !onlyPartitionsWithMessages || p.Messages.Length > 0).AsEnumerable()));
            }
        }

        public class PartitionFetchData
        {
            public int Partition;
            public ErrorCode ErrorCode;
            public long HighWatermarkOffset;
            public Message[] Messages;

            public override string ToString()
            {
                return string.Format("{0}:{1}:{2}:{3}", Partition, ErrorCode, HighWatermarkOffset, Messages == null ? "null" : Messages.Length.ToString());
            }
        }

        public override string ToString()
        {
            return ToString(false);
        }

        public string ToString(bool onlyPartitionsWithMessages)
        {
            if (Topics == null)
                return "null";

            return string.Format("[{0}]", string.Join("\n ", Topics.Select(t=>t.ToString(onlyPartitionsWithMessages))));
        }
    }
}
