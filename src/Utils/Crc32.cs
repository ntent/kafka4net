using System;

namespace kafka4net.Utils
{
    /// <summary>Simplfied version of http://damieng.com/blog/2006/08/08/calculating_crc32_in_c_and_net</summary>
    static class Crc32
    {
        const UInt32 _polynomial = 0xEDB88320u;
        static readonly UInt32[] _table = InitializeTable();

        public static UInt32 Update(byte[] buffer, UInt32 state = ~0U, int len = -1, int offset = 0)
        {
            if (len == -1)
                len = buffer.Length;

            for (int i = offset; i < offset + len; i++)
                state = (state >> 8) ^ _table[buffer[i] ^ state & 0xff];

            return state;
        }

        public static UInt32 Update(int i, UInt32 state)
        {
            state = (state >> 8) ^ _table[(i >> 8 * 3 & 0xff) ^ state & 0xff];
            state = (state >> 8) ^ _table[(i >> 8 * 2 & 0xff) ^ state & 0xff];
            state = (state >> 8) ^ _table[(i >> 8 & 0xff) ^ state & 0xff];
            state = (state >> 8) ^ _table[(i & 0xff) ^ state & 0xff];
            return state;
        }

        public static UInt32 Update(byte b, UInt32 state = ~0U)
        {
            state = (state >> 8) ^ _table[b ^ state & 0xff];
            return state;
        }

        public static UInt32 GetHash(UInt32 state)
        {
            return ~state;
        }

        private static UInt32[] InitializeTable()
        {
            var createTable = new UInt32[256];
            for (var i = 0; i < 256; i++)
            {
                var entry = (UInt32)i;
                for (var j = 0; j < 8; j++)
                    if ((entry & 1) == 1)
                        entry = (entry >> 1) ^ _polynomial;
                    else
                        entry = entry >> 1;
                createTable[i] = entry;
            }

            return createTable;
        }

    }
}
