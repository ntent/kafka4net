namespace kafka4net
{
    public class Message
    {
        public byte[] Key;
        public byte[] Value;
        
        internal long Offset;
        
        /// <summary>
        /// Not part of the protocol. Is used to group messages belonging to the same partition before sending.
        /// Is set by Publisher before enqueing to partition queue.
        /// </summary>
        internal int PartitionId { get; set; }
    }
}
