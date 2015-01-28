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
            public int MaxNumOffsets;

            public override string ToString()
            {
                return string.Format("[{0}:{1}:{2}]", Id, Time, MaxNumOffsets);
            }
        }

        public override string ToString()
        {
            return string.Format("{0} Id:Time:MaxNumOffsets [{1}]", TopicName, Partitions == null ? "null" : string.Join("\n ", Partitions.AsEnumerable()));
        }
    }
}
