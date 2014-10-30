namespace kafka4net
{
    public class PartitionInfo
    {
        public int Partition;
        /// <summary>If queue is empty than -1</summary>
        public long Head;
        public long Tail;

        public override string ToString()
        {
            return string.Format("Partition: {0} Head: {1} Tail {2}", Partition, Head, Tail);
        }
    }
}
