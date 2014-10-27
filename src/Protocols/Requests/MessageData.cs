namespace kafka4net.Protocols.Requests
{
    class MessageData
    {
        /// <summary>This byte holds metadata attributes about the message. The lowest 2 bits contain 
        /// the compression codec used for the message. The other bits should be set to 0</summary>
        //public byte Attributes;

        /// <summary> The key is an optional message key that was used for partition assignment. 
        /// The key can be null</summary>
        public byte[] Key;
        /// <summary>The value is the actual message contents as an opaque byte array. 
        /// Kafka supports recursive messages in which case this may itself contain a message set. 
        /// The message can be null</summary>
        public byte[] Value;
    }
}
