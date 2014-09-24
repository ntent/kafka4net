namespace kafka4net.Protocol.Requests
{
    class OffsetRequest
    {
        public string TopicName;
        public int[] Partitions;
        /// <summary> -1 for last offset, -2 for first offset, otherwise time</summary>
        public long Time;
    }
}
