using kafka4net.Metadata;
using System;

namespace kafka4net
{
    /// <summary>
    /// Default message partitioner. Uses a Fletcher hash to assign given message key to a partition. If no key, then randomly selects a partition.
    /// </summary>
    internal class FletcherHashedMessagePartitioner : IMessagePartitioner
    {
        private readonly Random _rnd = new Random();

        public PartitionMeta GetMessagePartition(Message message, PartitionMeta[] allPartitions)
        {
            var index = message.Key == null ?
                _rnd.Next(allPartitions.Length) :
            Fletcher32HashOptimized(message.Key) % allPartitions.Length;

            return allPartitions[index];

        }

        /// <summary>Optimized Fletcher32 checksum implementation.
        /// <see cref="http://en.wikipedia.org/wiki/Fletcher%27s_checksum#Fletcher-32"/></summary>
        private uint Fletcher32HashOptimized(byte[] msg)
        {
            if (msg == null)
                return 0;
            var words = msg.Length;
            int i = 0;
            uint sum1 = 0xffff, sum2 = 0xffff;

            while (words != 0)
            {
                var tlen = words > 359 ? 359 : words;
                words -= tlen;
                do
                {
                    sum2 += sum1 += msg[i++];
                } while (--tlen != 0);
                sum1 = (sum1 & 0xffff) + (sum1 >> 16);
                sum2 = (sum2 & 0xffff) + (sum2 >> 16);
            }
            /* Second reduction step to reduce sums to 16 bits */
            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
            return (sum2 << 16 | sum1);
        }

    }
}
