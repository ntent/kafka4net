namespace kafka4net.Protocol.Responses
{
    class ProducerResponse
    {
        internal TopicResponse[] Topics;

        internal class TopicResponse
        {
            public string TopicName;
            public PartitionResponse[] Partitions;
        }

        internal class PartitionResponse
        {
            public int Partition;
            public ErrorCode ErrorCode;
            public long Offset;
        }
    }
}
