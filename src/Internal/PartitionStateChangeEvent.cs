using System;

namespace kafka4net.Internal
{
    internal class PartitionStateChangeEvent
    {
        public PartitionStateChangeEvent(string topic, int partitionId, ErrorCode errorCode)
        {
            Topic = topic;
            PartitionId = partitionId;
            ErrorCode = errorCode;
        }

        public readonly string Topic;
        public readonly int PartitionId;
        public readonly ErrorCode ErrorCode;

        public override bool Equals(object obj)
        {
            if (obj == null)
                return false;

            var pse = obj as PartitionStateChangeEvent;
            if (pse == null)
                return false;

            return Topic == pse.Topic && PartitionId == pse.PartitionId && ErrorCode == pse.ErrorCode;
        }

        public override int GetHashCode()
        {
            unchecked // disable overflow, for the unlikely possibility that you
            {         // are compiling with overflow-checking enabled
                int hash = 27;
                hash = (13 * hash) + Topic.GetHashCode();
                hash = (13 * hash) + PartitionId.GetHashCode();
                hash = (13 * hash) + ErrorCode.GetHashCode();
                return hash;
            }
        }

        public override string ToString()
        {
            return string.Format("PartitionStateChangeEvent: '{0}'/{1}/{2}", Topic, PartitionId, ErrorCode);
        }
    }
}
