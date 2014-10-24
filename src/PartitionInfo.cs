namespace kafka4net
{
    public class PartitionInfo
    {
        public int Partition;
        /// <summary>If queue is empty than -1</summary>
        public long Head;
        public long Tail;
    }
}
