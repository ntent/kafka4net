using System.Linq;

namespace kafka4net.Protocols.Requests
{
    class OffsetRequest
    {
        public string TopicName;
        public PartitionData[] Partitions;
        /// <summary> -1 for last offset, -2 for first offset, otherwise time</summary>
        //public long Time;

        public class PartitionData
        {
            public int Id;
            public long Time;
        }

        public override string ToString()
        {
            return string.Format("{0} [{1}]", TopicName, string.Join(",", Partitions.Select(p=>p.Id)));
        }
    }
}
