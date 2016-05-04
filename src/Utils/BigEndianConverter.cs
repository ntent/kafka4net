using System;
using System.IO;

namespace kafka4net.Utils
{
    static class BigEndianConverter
    {
        public static int ReadInt32(Stream s)
        {
            if(s.CanSeek && s.Position + 4 > s.Length)
                throw new Exception(string.Format("ReadInt32 needs 4 bytes but got ony {0}", s.Length - s.Position));
            return s.ReadByte() << 3 * 8 | s.ReadByte() << 2 * 8 | s.ReadByte() << 8 | s.ReadByte();
        }

        public static int ReadInt32(byte[] buff, int offset=0)
        {
            return buff[offset] << 3 * 8 | buff[offset + 1] << 2 * 8 | buff[offset+2] << 8 | buff[offset+3];
        }


        public static short ReadInt16(Stream s)
        {
            if (s.CanSeek && s.Position + 2 > s.Length)
                throw new Exception(string.Format("ReadInt16 needs 2 bytes but got ony {0}", s.Length - s.Position));
            return (short)((s.ReadByte() << 8) | s.ReadByte());
        }

        public static long ReadInt64(Stream stream)
        {
            if (stream.CanSeek && stream.Position + 8 > stream.Length)
                throw new Exception(string.Format("ReadInt64 needs 8 bytes but got ony {0}", stream.Length - stream.Position));

            var res = 0L;
            for (int i = 0; i < 8; i++)
                res = res << 8 | stream.ReadByte();
            return res;
        }

        public static void Write(Stream stream, long i)
        {
            ulong ui = (ulong)i;
            for (int j = 7; j >= 0; j--)
                stream.WriteByte((byte)(ui >> j * 8 & 0xff));
        }

        public static void Write(Stream stream, int i)
        {
            WriteByte(stream, i >> 8 * 3);
            WriteByte(stream, i >> 8 * 2);
            WriteByte(stream, i >> 8);
            WriteByte(stream, i);
        }

        public static void Write(Stream stream, short i)
        {
            WriteByte(stream, i >> 8);
            WriteByte(stream, i);
        }

        public static void WriteByte(Stream stream, int i)
        {
            stream.WriteByte((byte)(i & 0xff));
        }

        public static void Write(byte[] buff, int i)
        {
            buff[0] = (byte)(i >> 8 * 3);
            buff[1] = (byte)((i & 0xff0000) >> 8 * 2);
            buff[2] = (byte)((i & 0xff00) >> 8);
            buff[3] = (byte)(i & 0xff);
        }

        public static int ToInt32(byte[] buff)
        {
            return (buff[0] << 8 * 3) | (buff[1] << 8 * 2) | (buff[2] << 8) | buff[3];
        }
    }
}
