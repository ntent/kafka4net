namespace kafka4net.ConsumerImpl
{
    class PartitionFetchState
    {
        public readonly int PartId;
        public readonly long Time;
        public long Offset;

        public PartitionFetchState(int partId, long time, long offset)
        {
            PartId = partId;
            Time = time;
            Offset = offset;
        }

        public override string ToString()
        {
            return string.Format("Part: {0}, Offset: {1} Time: {2}", PartId, Offset, Time);
        }
    }
}
