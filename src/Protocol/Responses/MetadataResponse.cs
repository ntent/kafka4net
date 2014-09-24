using kafka4net.Metadata;

namespace kafka4net.Protocol.Responses
{
    class MetadataResponse
    {
        public BrokerMeta[] Brokers;
        public TopicMeta[] Topics;
    }
}
