using System.Collections.Generic;

namespace kafka4net.Protocol.Requests
{
    class TopicData
    {
        public string TopicName;
        public IEnumerable<PartitionData> PartitionsData;
    }
}
