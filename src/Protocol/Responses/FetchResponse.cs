namespace kafka4net.Protocol.Responses
{
    class FetchResponse
    {
        public TopicFetchData[] Topics;

        public class TopicFetchData
        {
            public string Topic;
            public PartitionFetchData[] Partitions;
        }

        public class PartitionFetchData
        {
            public int Partition;
            public ErrorCode ErrorCode;
            public long HighWatermarkOffset;
            public Message[] Messages;
        }
    }
}
