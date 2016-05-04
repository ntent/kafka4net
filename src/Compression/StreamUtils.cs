using System.IO;

namespace kafka4net.Compression
{
    static class StreamUtils
    {
        internal static bool ReadAll(Stream stream, byte[] dst, int count)
        {
            while (true)
            {
                var res = stream.Read(dst, 0, count);
                count -= res;
                if (count == 0)
                    return true;
                if (res == 0)
                    return false;
            }
        }
    }
}
