
namespace kafka4net.Utils
{
    internal static class LittleEndianConverter
    {
        public static uint ReadUInt32(byte[] buff, int offset)
        {
            return (uint)(buff[offset+3] << 3 * 8 | buff[offset+2] << 2 * 8 | buff[offset+1] << 8 | buff[offset]);
        }
    }
}
