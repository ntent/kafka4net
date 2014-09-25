using System.Collections.Generic;

namespace kafka4net.Protocol.Requests
{
    class TopicData
    {
        public string TopicName;
        public IEnumerable<PartitionData> PartitionsData;

        public override string ToString()
        {
            return string.Format("Topic: {0} Parts: [{1}]", TopicName, string.Join(", ", PartitionsData));
        }
    }
}
