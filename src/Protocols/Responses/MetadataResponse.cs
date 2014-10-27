using kafka4net.Metadata;

namespace kafka4net.Protocols.Responses
{
    class MetadataResponse
    {
        public BrokerMeta[] Brokers;
        public TopicMeta[] Topics;
    }
}
