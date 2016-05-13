
namespace kafka4net.Utils
{
    internal static class LittleEndianConverter
    {
        public static uint ReadUInt32(byte[] buff, int offset)
        {
            return (uint)(buff[offset+3] << 3 * 8 | buff[offset+2] << 2 * 8 | buff[offset+1] << 8 | buff[offset]);
        }

        public static void Write(uint i, byte[] buff, int offset)
        {
            buff[offset] = (byte)(i & 0xff);
            buff[offset + 1] = (byte)(i >> 8 & 0xff);
            buff[offset + 2] = (byte)(i >> 8*2 & 0xff);
            buff[offset + 3] = (byte)(i >> 8*3 & 0xff);
        }
    }
}
