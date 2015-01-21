using System.Linq;

namespace kafka4net.Metadata
{
    public class PartitionMeta
    {
        /// <summary>
        /// ReplicaNotAvailable is not really an error. Use Success() function instead of checking this value
        /// </summary>
        public ErrorCode RawErrorCode;
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
            return string.Format("Id: {0} Leader: {1} Error: {2}, Replicas: {3}, Isr: {4}", Id, Leader, RawErrorCode, IntToStringList(Replicas), IntToStringList(Isr));
        }

        static string IntToStringList(int[] ints)
        {
            if (ints == null)
                return "";
            return string.Join(",", ints.Select(i => i.ToString()).ToArray());
        }

        public bool Success()
        {
            return RawErrorCode == ErrorCode.NoError || RawErrorCode == ErrorCode.ReplicaNotAvailable;
        }

        /// <summary>
        /// Copare error codes, ignoring ReplicaNotAvailable
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool DifferentErrorCode(PartitionMeta other)
        {
            var c1 = RawErrorCode;
            var c2 = other.RawErrorCode;
            
            if(c1 == ErrorCode.ReplicaNotAvailable)
                c1 = ErrorCode.NoError;
            if(c2 == ErrorCode.ReplicaNotAvailable)
                c2 = ErrorCode.NoError;

            return c1 != c2;
        }
    }
}
