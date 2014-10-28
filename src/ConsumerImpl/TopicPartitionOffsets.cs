using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace kafka4net.ConsumerImpl
{
    /// <summary>
    /// Container Class to aid in storing offsets for all partitions in a topic
    /// </summary>
    public class TopicPartitionOffsets
    {
        public readonly string Topic;
        private readonly Dictionary<int, long> _offsets;

        public TopicPartitionOffsets(string topic, Dictionary<int,long> initialOffsets)
        {
            Topic = topic;
            _offsets = new Dictionary<int, long>(initialOffsets);
        }

        /// <summary>
        /// Construct a new set of offsets from the given stream. It is assumed that the stream contains serialized data created with WriteOffsets and is positioned at the start of this data.
        /// </summary>
        /// <param name="inputStream"></param>
        public TopicPartitionOffsets(Stream inputStream)
        {
            var reader = new BinaryReader(inputStream);
            Topic = reader.ReadString();

            // read from the input stream and initialize the offsets.
            _offsets = ReadOffsets(reader);
        }

        /// <summary>
        /// Get the offset for the given partition ID
        /// </summary>
        /// <param name="partitionId"></param>
        /// <returns>the offset</returns>
        public long ResolveOffset(int partitionId)
        {
            long offset;
            if (_offsets.TryGetValue(partitionId, out offset))
                return offset;

            throw new Exception(string.Format("Partition ID {0} does not exist in offsets for topic {1}", partitionId, Topic));
        }


        /// <summary>
        /// Serialize all offsets in this object to the given stream
        /// </summary>
        /// <param name="stream"></param>
        public void WriteOffsets(Stream stream)
        {
            var writer = new BinaryWriter(stream);
            writer.Write(Topic);
            throw new NotImplementedException();
        }

        /// <summary>
        /// Deserialize all offsets in this object from the given stream. 
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private Dictionary<int, long> ReadOffsets(BinaryReader reader)
        {
            throw new NotImplementedException();
        }
    }
}
