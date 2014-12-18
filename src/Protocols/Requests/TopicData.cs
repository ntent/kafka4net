using System.Collections.Generic;

namespace kafka4net.Protocols.Requests
{
    class TopicData
    {
        public string TopicName;
        public IEnumerable<PartitionData> PartitionsData;

        public override string ToString()
        {
            var partDataStr = PartitionsData == null ? "" : string.Join(", ", PartitionsData);
            return string.Format("Topic: {0} Parts: [{1}]", TopicName, partDataStr);
        }
    }
}
