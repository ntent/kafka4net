using System.Linq;

namespace kafka4net.Metadata
{
    public class PartitionMeta
    {
        public ErrorCode ErrorCode;
        public int Id;
        
        /// <summary>
        /// The node id for the kafka broker currently acting as leader for this partition. 
        /// If no leader exists because we are in the middle of a leader election this id will be -1.
        /// </summary>
        public int Leader;

        /// <summary>The set of alive nodes that currently acts as slaves for the leader for this partition</summary>
        public int[] Replicas;

        /// <summary>The set subset of the replicas that are "caught up" to the leader</summary>
        public int[] Isr;

        public override string ToString()
        {
            return string.Format("Id: {0} Leader: {1} Error: {2}, Replicas: {3}, Isr: {4}", Id, Leader, ErrorCode, IntToStringList(Replicas), IntToStringList(Isr));
        }

        static string IntToStringList(int[] ints)
        {
            if (ints == null)
                return "";
            return string.Join(",", ints.Select(i => i.ToString()).ToArray());
        }
    }
}
