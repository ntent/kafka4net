namespace kafka4net.Protocol.Requests
{
    // TODO: delete this class, its unused
    class MessageSetItem
    {
        /// <summary> This is the offset used in kafka as the log sequence number. When the producer is sending 
        /// messages it doesn't actually know the offset and can fill in any value here it likes</summary>
        public long Offset;
        public int MessageSize;
        public MessageData MessageData;
    }
}
