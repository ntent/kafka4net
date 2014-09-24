namespace kafka4net
{
    public class Message
    {
        public byte[] Key;
        public byte[] Value;
        
        internal long Offset;
    }
}
