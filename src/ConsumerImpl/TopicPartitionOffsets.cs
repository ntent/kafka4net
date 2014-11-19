using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace kafka4net.ConsumerImpl
{
    /// <summary>
    /// Container Class to aid in storing offsets for all partitions in a topic. The class stores internally the NEXT offset to process for a partition. When the topic is empty, the offsets stored would be 0 (the next offset to fetch)
    /// When initialized to the TopicTail, it will contain the offset of the NEXT message to be written to the partition.
    /// </summary>
    public class TopicPartitionOffsets
    {
        public readonly string Topic;
        private readonly Dictionary<int, long> _offsets;

        /// <summary>
        /// Initializes a TopicPartitionOffsets structure from a dictionary. The offsets in the dictionary must be the NEXT offset to process for each partition, not the *Current* offset.
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="initialOffsets"></param>
        internal TopicPartitionOffsets(string topic, Dictionary<int,long> initialOffsets)
        {
            Topic = topic;
            _offsets = new Dictionary<int, long>(initialOffsets);
        }


        internal TopicPartitionOffsets(string topic)
        {
            Topic = topic;
            _offsets = new Dictionary<int, long>();
        }

        /// <summary>
        /// Construct a new set of offsets from the given stream. It is assumed that the stream contains serialized data created with WriteOffsets and is positioned at the start of this data.
        /// </summary>
        /// <param name="serializedBytes"></param>
        public TopicPartitionOffsets(byte[] serializedBytes) : this(new MemoryStream(serializedBytes))
        {
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
        /// Get the next offset to request for the given partition ID (current offset + 1)
        /// </summary>
        /// <param name="partitionId"></param>
        /// <returns>the next offset for the given partition</returns>
        public long NextOffset(int partitionId)
        {
            long offset;
            if (_offsets.TryGetValue(partitionId, out offset))
                return offset;

            throw new Exception(string.Format("Partition ID {0} does not exist in offsets for topic {1}", partitionId, Topic));
        }

        public IEnumerable<int> Partitions { get { return new List<int>(_offsets.Keys); }}

        /// <summary>
        /// Update the offset for the given partition ID to include the given offset. This will cause offset+1 to be returned by the NextOffset function.
        /// Note that this function expects to be passed the *Current* offset that has been processed.
        /// </summary>
        /// <param name="partitionId"></param>
        /// <param name="offset"></param>
        public void UpdateOffset(int partitionId, long offset)
        {
            _offsets[partitionId] = offset+1;
        }

        /// <summary>
        /// Returns the total number of messages difference between this set of offsets, and the given set.
        /// </summary>
        /// <param name="priorOffsets">The earlier offsets. If the passed offsets are larger than this instance offsets, the result will be negative.</param>
        /// <returns></returns>
        public long MessagesSince(TopicPartitionOffsets priorOffsets)
        {
            if (_offsets.Count != priorOffsets._offsets.Count || Topic != priorOffsets.Topic)
                throw new ArgumentException("priorOffsets does not match Topic or Number of Partitions of this set of offsets.");

            return _offsets.Keys.Select(p => _offsets[p] - priorOffsets._offsets[p]).Sum();
        }

        /// <summary>
        /// Serialize all offsets in this object to the given stream
        /// </summary>
        /// <param name="stream"></param>
        public void WriteOffsets(Stream stream)
        {
            var writer = new BinaryWriter(stream);
            writer.Write(Topic);
            // number of offsets
            writer.Write(_offsets.Count);
            foreach (var partOffset in _offsets)
            {
                writer.Write(partOffset.Key);
                writer.Write(partOffset.Value);
            }
        }

        public byte[] WriteOffsets()
        {
            var memStream = new MemoryStream(800);
            WriteOffsets(memStream);
            return memStream.ToArray();
        }

        /// <summary>
        /// Deserialize all offsets in this object from the given stream. 
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        private Dictionary<int, long> ReadOffsets(BinaryReader reader)
        {
            // number of offsets
            var offsetCount = reader.ReadInt32();
            var offsets = new Dictionary<int, long>(offsetCount);
            for (var i = 0; i < offsetCount; i++)
            {
                offsets.Add(reader.ReadInt32(),reader.ReadInt64());
            }
            return offsets;
        }
    }
}
