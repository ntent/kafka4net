using System.Linq;
using kafka4net.Metadata;

namespace kafka4net.Protocols.Responses
{
    class MetadataResponse
    {
        public BrokerMeta[] Brokers;
        public TopicMeta[] Topics;

        public override string ToString()
        {
            return string.Format("Brokers: [{0}], TopicMeta: [{1}]", string.Join(",", Brokers.AsEnumerable()), string.Join(",", Topics.AsEnumerable()));
        }
    }
}
