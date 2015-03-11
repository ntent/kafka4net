namespace kafka4net.ConsumerImpl
{
    class PartitionFetchState
    {
        public readonly int PartId;
        public readonly ConsumerLocation StartLocation;
        public long Offset;

        public PartitionFetchState(int partId, ConsumerLocation startLocation, long offset)
        {
            PartId = partId;
            StartLocation = startLocation;
            Offset = offset;
        }

        public override string ToString()
        {
            return string.Format("Part: {0}, Offset: {1} StartLocation: {2}", PartId, Offset, StartLocation);
        }
    }
}
