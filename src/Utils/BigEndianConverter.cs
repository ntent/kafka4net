using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka4net.Utils
{
    static class BigEndianConverter
    {
        public static int ReadInt32(MemoryStream s)
        {
            return s.ReadByte() << 3 * 8 | s.ReadByte() << 2 * 8 | s.ReadByte() << 8 | s.ReadByte();
        }

        public static short ReadInt16(MemoryStream s)
        {
            return (short)((s.ReadByte() << 8) | s.ReadByte());
        }

        public static long ReadInt64(MemoryStream stream)
        {
            var res = 0L;
            for (int i = 0; i < 8; i++)
                res = res << 8 | stream.ReadByte();
            return res;
        }

        public static void Write(MemoryStream stream, long i)
        {
            ulong ui = (ulong)i;
            for (int j = 7; j >= 0; j--)
                stream.WriteByte((byte)(ui >> j * 8 & 0xff));
        }

        public static void Write(MemoryStream stream, int i)
        {
            WriteByte(stream, i >> 8 * 3);
            WriteByte(stream, i >> 8 * 2);
            WriteByte(stream, i >> 8);
            WriteByte(stream, i);
        }

        public static void Write(MemoryStream stream, short i)
        {
            WriteByte(stream, i >> 8);
            WriteByte(stream, i);
        }

        public static void WriteByte(MemoryStream stream, int i)
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
