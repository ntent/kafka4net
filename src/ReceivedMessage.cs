namespace kafka4net
{
    public class ReceivedMessage
    {
        public string Topic;
        public int Partition;
        public byte[] Key;
        public byte[] Value;
        public long Offset;
        public long HighWaterMarkOffset;
    }
}
