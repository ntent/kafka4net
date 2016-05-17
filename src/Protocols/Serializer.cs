using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using kafka4net.Compression;
using kafka4net.Metadata;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using kafka4net.Utils;

namespace kafka4net.Protocols
{
    /// <summary>
    /// Compression buffers are allocated as thread static in order to minimaze buffer allocation.
    /// This is safe because serializer is called from Protocol's socket loop which is synchronized by EventLoopScheduler, which is single-threaded.
    /// </summary>
    static class Serializer
    {
        static readonly byte[] _minusOne32 = { 0xff, 0xff, 0xff, 0xff };
        static readonly byte[] _one32 = { 0x00, 0x00, 0x00, 0x01 };
        static readonly byte[] _minusOne16 = { 0xff, 0xff };
        static readonly byte[] _apiVersion = { 0x00, 0x00 };
        static readonly byte[] _zero64 = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        static readonly byte[] _zero32 = { 0x00, 0x00, 0x00, 0x00 };
        // Byte representation of CompressionType
        static readonly byte[] _compressionEnumTable = {
            0x00,   // None
            0x01,   // Gzip
            0x02,   // Snappy
            0x03    // Lz4
        };
        // TODO: make it configurable
        static byte[] _clientId;

        private static readonly ILogger _log = Logger.GetLogger();

        static Serializer()
        {
            var ms = new MemoryStream();
            Write(ms, "kafka4net");
            _clientId = ms.ToArray();
        }

        enum ApiKey : short
        {
            ProduceRequest = 0,
            FetchRequest = 1,
            OffsetRequest = 2,
            MetadataRequest = 3,
            // Non-user facing control APIs 4-7
            OffsetCommitRequest = 8,
            OffsetFetchRequest = 9,
            ConsumerMetadataRequest	= 10
        }

        public static MetadataResponse DeserializeMetadataResponse(byte[] body, int len)
        {
            var stream = new MemoryStream(body, 0 , len);
            stream.Position += 4; 
            var ret = new MetadataResponse();
            
            var count = BigEndianConverter.ReadInt32(stream);
            ret.Brokers = new BrokerMeta[count];
            for (int i = 0; i < count; i++)
                ret.Brokers[i] = DeserializeBrokerMeta(stream);

            count = BigEndianConverter.ReadInt32(stream);
            ret.Topics = new TopicMeta[count];
            for (int i = 0; i < count; i++)
                ret.Topics[i] = DeserializeTopicMeta(stream);

            return ret;
        }

        private static TopicMeta DeserializeTopicMeta(MemoryStream stream)
        {
            var ret = new TopicMeta();
            ret.ErrorCode = (ErrorCode)BigEndianConverter.ReadInt16(stream);
            ret.TopicName = ReadString(stream);

            var count = BigEndianConverter.ReadInt32(stream);
            ret.Partitions = new PartitionMeta[count];
            for (int i = 0; i < count; i++)
                ret.Partitions[i] = DeserializePartitionMeta(stream);

            return ret;
        }

        private static PartitionMeta DeserializePartitionMeta(MemoryStream stream)
        {
            var ret = new PartitionMeta();
            ret.ErrorCode = (ErrorCode)BigEndianConverter.ReadInt16(stream);
            ret.Id = BigEndianConverter.ReadInt32(stream);
            ret.Leader = BigEndianConverter.ReadInt32(stream);

            var count = BigEndianConverter.ReadInt32(stream);
            ret.Replicas = new int[count];
            for (int i = 0; i < count; i++)
                ret.Replicas[i] = BigEndianConverter.ReadInt32(stream);

            count = BigEndianConverter.ReadInt32(stream);
            ret.Isr = new int[count];
            for (int i = 0; i < count; i++)
                ret.Isr[i] = BigEndianConverter.ReadInt32(stream);

            return ret;
        }

        private static BrokerMeta DeserializeBrokerMeta(MemoryStream stream)
        {
            return new BrokerMeta {
                NodeId = BigEndianConverter.ReadInt32(stream), 
                Host = ReadString(stream), 
                Port = BigEndianConverter.ReadInt32(stream)
            };
        }

        public static byte[] Serialize(TopicRequest request, int correlationId)
        {
            var stream = new MemoryStream();
            WriteRequestHeader(stream, correlationId, ApiKey.MetadataRequest);
            if (request.Topics == null || request.Topics.Length == 0)
            {
                stream.Write(_zero32, 0, 4);
            }
            else
            {
                BigEndianConverter.Write(stream, request.Topics.Length);
                foreach (var t in request.Topics)
                    Write(stream, t);
            }

            stream.Close();

            return WriteMessageLength(stream);
        }

        private static byte[] WriteMessageLength(MemoryStream stream)
        {
            var buff = stream.ToArray();
            var len = buff.Length - 4; // -4 because do not count size flag itself
            // write message length to the head
            // TODO: use seek?
            BigEndianConverter.Write(buff, len);
            return buff;
        }

        public static byte[] Serialize(ProduceRequest request, int correlationId)
        {
            var stream = new MemoryStream();
            WriteRequestHeader(stream, correlationId, ApiKey.ProduceRequest);
            BigEndianConverter.Write(stream, request.RequiredAcks);
            BigEndianConverter.Write(stream, request.Timeout);
            WriteArray(stream, request.TopicData, t => Write(stream, t));
            return WriteMessageLength(stream);
        }

        private static void Write(MemoryStream stream, TopicData topic)
        {
            Write(stream, topic.TopicName);
            WriteArray(stream, topic.PartitionsData, p => Write(stream, p));
        }

        private static void Write(MemoryStream stream, PartitionData partition)
        {
            BigEndianConverter.Write(stream, partition.Partition);
            if (partition.CompressionType == CompressionType.None)
            {
                var messageArray = partition.Messages.ToArray();  // make sure linq evaluation happen just once
                var messageSetSize = EstimateMessageSetSize(messageArray);
                BigEndianConverter.Write(stream, messageSetSize);

                Write(stream, partition.Messages);
            }
            else
            {
                var compressedMessage = Compress(partition.CompressionType, partition.Messages);
                WriteCompressedMessageSet(stream, compressedMessage, partition.CompressionType);
            }
        }

        [ThreadStatic] static byte[] _snappyUncompressedBuffer;
        [ThreadStatic] static byte[] _snappyCompressedBuffer;
        [ThreadStatic] static MemoryStream compressed;
        static MessageData Compress(CompressionType compressionType, IEnumerable<MessageData> messages)
        {
            if (compressed == null)
            {
                compressed = new MemoryStream();
            }
            else
            {
                compressed.Position = 0;
                compressed.SetLength(0);
            }

            switch (compressionType)
            {
                case CompressionType.Gzip:
                    {
                        var gzip = new GZipStream(compressed, CompressionLevel.Optimal, true);
                        Write(gzip, messages);
                        gzip.Close();
                        break;
                    }
                    case CompressionType.Snappy:
                    {
                        if(_snappyCompressedBuffer == null)
                            KafkaSnappyStream.AllocateBuffers(out _snappyUncompressedBuffer, out _snappyCompressedBuffer);

                        var snappy = new KafkaSnappyStream(compressed, CompressionStreamMode.Compress, _snappyUncompressedBuffer, _snappyCompressedBuffer);
                        Write(snappy, messages);
                        snappy.Close();
                        break;
                    }
                    case CompressionType.Lz4:
                    {
                        var lz4 = new Lz4KafkaStream(compressed, CompressionStreamMode.Compress);
                        Write(lz4, messages);
                        lz4.Close();
                        break;
                    }
                default:
                    throw new NotImplementedException($"Compression '{compressionType}' is not implemented");
            }

            var buff = new ArraySegment<byte>(compressed.GetBuffer(), 0, (int)compressed.Length);
            return new MessageData {Key = null, Value = buff };
        }

        private static void Write(Stream stream, IEnumerable<MessageData> messages)
        {
            foreach (var message in messages)
            {
                stream.Write(_zero64, 0, 8); // producer does fake offset

                var messageSize = _messageOverheadSize + (message.Key?.Length ?? 0) + message.Value.Count;
                BigEndianConverter.Write(stream, messageSize);

                Write(stream, message);
            }
        }

        static void WriteCompressedMessageSet(Stream stream, MessageData message, CompressionType compresionType)
        {
            var messageRawSize = 0;
            if (message.Key != null)
                messageRawSize += message.Key.Length;
            if (message.Value != null)
                messageRawSize += message.Value.Count;

            var messageSetSize = _messageSetOverheadSize + messageRawSize;
            BigEndianConverter.Write(stream, messageSetSize);
            _log.Debug("WriteCompressedMessageSet: size: {0}", messageSetSize);

            stream.Write(_zero64, 0, 8);    // Producer fake offset

            var messageSize = _messageOverheadSize + messageRawSize;
            BigEndianConverter.Write(stream, messageSize);

            Write(stream, message, compresionType);
        }

        const int _messageOverheadSize =
                + 4 // crc
                + 1 // magic
                + 1 // attributes
                + 4 * 2; // key size plus value size

        const int _messageSetOverheadSize = 
                8   // offset
                + 4 // message size field
                + _messageOverheadSize;

        static int EstimateMessageSetSize(MessageData[] messages)
        {
            var size = _messageSetOverheadSize * messages.Length;

            foreach (var message in messages)
            {
                if (message.Key != null)
                    size += message.Key.Length;
                size += message.Value.Count;
            }

            return size;
        }

        private static void Write(Stream stream, MessageData message, CompressionType compression = CompressionType.None)
        {
            var crc = Crc32.Update(_zero32, len: 1);
            crc = Crc32.Update(_compressionEnumTable, crc, 1, (int)compression);
            if (message.Key == null) {
                crc = Crc32.Update(_minusOne32, crc);
            }
            else {
                crc = Crc32.Update(message.Key.Length, crc);
                crc = Crc32.Update(message.Key, crc);
            }

            crc = Crc32.Update(message.Value.Count, crc);
            crc = Crc32.Update(message.Value, crc);
            crc = Crc32.GetHash(crc);
            BigEndianConverter.Write(stream, crc);

            stream.WriteByte(0); // magic byte
            stream.Write(_compressionEnumTable, (int)compression, 1); // attributes
            if (message.Key == null)
            {
                stream.Write(_minusOne32, 0, 4);
            }
            else
            {
                BigEndianConverter.Write(stream, message.Key.Length);
                stream.Write(message.Key, 0, message.Key.Length);
            }

            BigEndianConverter.Write(stream, message.Value.Count);
            stream.Write(message.Value.Array, 0, message.Value.Count);
        }

        static void WriteArray<T>(MemoryStream stream, IEnumerable<T> items, Action<T> write)
        {
            var sizePosition = stream.Position;
            stream.Write(_minusOne32, 0, 4); // placeholder for count field
            var count = 0;
            foreach (var item in items)
            {
                write(item);
                count++;
            }
            var pos = stream.Position; // update count field
            stream.Position = sizePosition;
            BigEndianConverter.Write(stream, count);
            stream.Position = pos;
        }

        private static void Write(MemoryStream stream, string s)
        {
            if(s == null) {
                stream.Write(_minusOne16, 0, 2);
                return;
            }

            BigEndianConverter.Write(stream, (short)s.Length);
            var b = Encoding.UTF8.GetBytes(s);
            stream.Write(b, 0, b.Length);
        }

        private static void WriteRequestHeader(MemoryStream stream, int correlationId, ApiKey requestType)
        {
            stream.Write(_minusOne32, 0, 4); // reserve space for message size
            BigEndianConverter.Write(stream, (short)requestType);
            stream.Write(_apiVersion, 0, 2);
            BigEndianConverter.Write(stream, correlationId);
            stream.Write(_clientId, 0, _clientId.Length);
        }

        public static ProducerResponse GetProducerResponse(byte[] buff, int len)
        {
            var resp = new ProducerResponse();
            var stream = new MemoryStream(buff, 0, len);
            stream.Position += 4; // skip message size

            var count = BigEndianConverter.ReadInt32(stream);
            resp.Topics = new ProducerResponse.TopicResponse[count];
            for (int i = 0; i < count; i++)
                resp.Topics[i] = DeserializeTopicResponse(stream);

            return resp;
        }

        private static ProducerResponse.TopicResponse DeserializeTopicResponse(MemoryStream stream)
        {
            var resp = new ProducerResponse.TopicResponse();
            resp.TopicName = ReadString(stream);
            var count = BigEndianConverter.ReadInt32(stream);
            resp.Partitions = new ProducerResponse.PartitionResponse[count];
            for (int i = 0; i < count; i++)
                resp.Partitions[i] = DeserializePartitionResponse(stream);
            return resp;
        }

        private static ProducerResponse.PartitionResponse DeserializePartitionResponse(MemoryStream stream)
        {
            return new ProducerResponse.PartitionResponse
            {
                Partition = BigEndianConverter.ReadInt32(stream),
                ErrorCode = (ErrorCode)BigEndianConverter.ReadInt16(stream),
                Offset = BigEndianConverter.ReadInt64(stream)
            };
        }

        private static string ReadString(MemoryStream stream)
        {
            var len = BigEndianConverter.ReadInt16(stream);
            // per contract, null string is represented with -1 len.
            if (len == -1)
                return null;
 
            var buffer = new byte[len];
            stream.Read(buffer, 0, len);
            return Encoding.UTF8.GetString(buffer);
        }

        internal static byte[] Serialize(OffsetRequest req, int correlationId)
        {
            var stream = new MemoryStream();

            WriteRequestHeader(stream, correlationId, ApiKey.OffsetRequest);
            stream.Write(_minusOne32, 0, 4);    // ReplicaId
            stream.Write(_one32, 0, 4);         // array of size 1: we send request for one topic
            Write(stream, req.TopicName);
            WriteArray(stream, req.Partitions, p =>
            {
                BigEndianConverter.Write(stream, p.Id);
                BigEndianConverter.Write(stream, p.Time);
                BigEndianConverter.Write(stream, p.MaxNumOffsets);
            });

            return WriteMessageLength(stream);
        }

        internal static OffsetResponse DeserializeOffsetResponse(byte[] buff, int count)
        {
            var stream = new MemoryStream(buff, 0, count);
            stream.Position += 4; // skip body

            var response = new OffsetResponse();

            var len = BigEndianConverter.ReadInt32(stream);
            // TODO: make sure connection is closed
            if(len != 1)
                throw new BrokerException("Invalid message format");

            response.TopicName = ReadString(stream);

            len = BigEndianConverter.ReadInt32(stream);
            response.Partitions = new OffsetResponse.PartitionOffsetData[len];
            for (int i = 0; i < len; i++)
            {
                var part = response.Partitions[i] = new OffsetResponse.PartitionOffsetData();
                part.Partition = BigEndianConverter.ReadInt32(stream);
                part.ErrorCode = (ErrorCode)BigEndianConverter.ReadInt16(stream);
                var len2 = BigEndianConverter.ReadInt32(stream);
                part.Offsets = new long[len2];
                for (int j = 0; j < len2; j++)
                    part.Offsets[j] = BigEndianConverter.ReadInt64(stream);
            }

            return response;
        }

        public static byte[] Serialize(FetchRequest req, int correlationId)
        {
            var stream = new MemoryStream();
            WriteRequestHeader(stream, correlationId, ApiKey.FetchRequest);
            stream.Write(_minusOne32, 0, 4);    // ReplicaId
            BigEndianConverter.Write(stream, req.MaxWaitTime);
            BigEndianConverter.Write(stream, req.MinBytes);

            WriteArray(stream, req.Topics, t => {
                Write(stream, t.Topic);
                WriteArray(stream, t.Partitions, p =>
                {
                    BigEndianConverter.Write(stream, p.Partition);
                    BigEndianConverter.Write(stream, p.FetchOffset);
                    BigEndianConverter.Write(stream, p.MaxBytes);
                });
            });

            return WriteMessageLength(stream);
        }

        /// <summary>
        /// FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
        ///  TopicName => string
        ///  Partition => int32
        ///  ErrorCode => int16
        ///  HighwaterMarkOffset => int64
        ///  MessageSetSize => int32
        /// </summary>
        /// <param name="buff"></param>
        /// <returns></returns>
        public static FetchResponse DeserializeFetchResponse(byte[] buff, int count)
        {
            var stream = new MemoryStream(buff, 0, count);
            stream.Position += 4; // skip body
            var response = new FetchResponse();

            var len = BigEndianConverter.ReadInt32(stream);
            
            response.Topics = new FetchResponse.TopicFetchData[len];
            for (int t = 0; t < len; t++)
            {
                var topic = new FetchResponse.TopicFetchData();
                response.Topics[t] = topic;
                topic.Topic = ReadString(stream);
                var len2 = BigEndianConverter.ReadInt32(stream);
                topic.Partitions = new FetchResponse.PartitionFetchData[len2];
                for (int i = 0; i < len2; i++)
                {
                    var partition = BigEndianConverter.ReadInt32(stream);
                    var error = (ErrorCode)BigEndianConverter.ReadInt16(stream);
                    var highWatermark = BigEndianConverter.ReadInt64(stream);
                    var messageSetSize = BigEndianConverter.ReadInt32(stream);
                    var messages = ReadMessageSet(stream, messageSetSize).ToArray();

                    topic.Partitions[i] = new FetchResponse.PartitionFetchData
                    {
                        Partition = partition,
                        ErrorCode = error,
                        HighWatermarkOffset = highWatermark,
                        Messages = messages
                    };
                }
            }

            return response;
        }


        //N.B., MessageSets are not preceded by an int32 like other array elements in the protocol.
        //
        //MessageSet => [Offset MessageSize Message]
        //  Offset => int64
        //  MessageSize => int32
        //
        //Message => Crc MagicByte Attributes Key Value
        //  Crc => int32
        //  MagicByte => int8
        //  Attributes => int8
        //  Key => bytes
        //  Value => bytes
        private static IEnumerable<Message> ReadMessageSet(Stream stream, int messageSetSize)
        {
            // "As an optimization the server is allowed to return a partial message at the end of the message set. 
            // Clients should handle this case"

            var remainingMessageSetBytes = messageSetSize;

            while (remainingMessageSetBytes > 0)
            {
                // we need at least be able to read offset and messageSize
                if (remainingMessageSetBytes < 8 + 4)
                {
                    // not enough bytes left. This is a partial message. Skip to the end of the message set.
                    stream.Position += remainingMessageSetBytes;
                    yield break;
                }

                var offset = BigEndianConverter.ReadInt64(stream);
                var messageSize = BigEndianConverter.ReadInt32(stream);
                
                // we took 12 bytes there, check again that we have a full message.
                remainingMessageSetBytes -= 8 + 4;
                if (remainingMessageSetBytes < messageSize)
                {
                    // not enough bytes left. This is a partial message. Skip to the end of the message set.
                    stream.Position += remainingMessageSetBytes;
                    yield break;
                }

                // Message
                var crc = (uint)BigEndianConverter.ReadInt32(stream);
                byte magic = (byte)stream.ReadByte();
                if(magic != 0)
                    throw new BrokerException("Invalid kafks message magic");  // TODO: use special exception for data corruption
                var attributes = (byte)stream.ReadByte();
                var compression = ParseCompression(attributes);
                var key = ReadByteArray(stream);
                var value = ReadByteArray(stream);
                if (compression == CompressionType.None)
                {
                    var msg = new Message();
                    msg.Key = key;
                    msg.Value = value;
                    msg.Offset = offset;

                    var computedCrc = Crc32.Update(magic);
                    computedCrc = Crc32.Update(attributes, computedCrc);
                    if (key == null) {
                        computedCrc = Crc32.Update(_minusOne32, computedCrc);
                    } else
                    {
                        computedCrc = Crc32.Update(key.Length, computedCrc);
                        computedCrc = Crc32.Update(key, computedCrc);
                    }
                    if(value == null)
                    {
                        computedCrc = Crc32.Update(_minusOne32);
                    }
                    else
                    {
                        computedCrc = Crc32.Update(value.Length, computedCrc);
                        computedCrc = Crc32.Update(value, computedCrc);
                    }
                    computedCrc = Crc32.GetHash(computedCrc);

                    if (computedCrc != crc)
                    {
                        throw new BrokerException(string.Format("Corrupt message: Crc does not match. Caclulated {0} but got {1}", computedCrc, crc));
                    }
                    yield return msg;
                }
                else if(compression == CompressionType.Gzip)
                {
                    var decompressedStream = new MemoryStream();
                    new GZipStream(new MemoryStream(value), CompressionMode.Decompress).CopyTo(decompressedStream);
                    decompressedStream.Seek(0, SeekOrigin.Begin);
                    // Recursion
                    var innerMessages = ReadMessageSet(decompressedStream, (int)decompressedStream.Length);
                    foreach (var innerMessage in innerMessages)
                        yield return innerMessage;
                }
                else if(compression == CompressionType.Lz4)
                {
                    using (var lz4Stream = new Lz4KafkaStream(new MemoryStream(value), CompressionStreamMode.Decompress))
                    {
                        var decompressed = new MemoryStream();
                        lz4Stream.CopyTo(decompressed);
                        decompressed.Seek(0, SeekOrigin.Begin);
                        var decompressedMessages = ReadMessageSet(decompressed, (int)decompressed.Length);
                        foreach (var msg in decompressedMessages)
                            yield return msg;
                    }
                }
                else if(compression == CompressionType.Snappy)
                {
                    if(_snappyCompressedBuffer == null)
                        KafkaSnappyStream.AllocateBuffers(out _snappyUncompressedBuffer, out _snappyCompressedBuffer);

                    using (var snappyStream = new KafkaSnappyStream(new MemoryStream(value), CompressionStreamMode.Decompress, _snappyUncompressedBuffer, _snappyCompressedBuffer))
                    {
                        var decompressed = new MemoryStream();
                        snappyStream.CopyTo(decompressed);
                        decompressed.Seek(0, SeekOrigin.Begin);
                        var decompressedMessages = ReadMessageSet(decompressed, (int)decompressed.Length);
                        foreach (var msg in decompressedMessages)
                            yield return msg;
                    }
                }
                else
                {
                    throw new BrokerException(string.Format("Unknown compression type: {0}", attributes & 3));
                }

                // subtract messageSize of that message from remaining bytes
                remainingMessageSetBytes -= messageSize;
            }
        }

        static void Dump(byte[] a)
        {
            var str1 = string.Join(" ", a.Select(_ => _.ToString("x2")).ToArray());
            Console.WriteLine(str1);

            var str = string.Join("\n", a.Select((_, i) =>
            {
                if(char.IsLetterOrDigit((char)_))
                    return $"{i} " + _.ToString("x2") + " " + ((char)_).ToString();
                else
                    return $"{i} " + _.ToString("x2") + " ?";
            }).ToArray());
            Console.WriteLine(str);
        }

        static CompressionType ParseCompression(int attributes)
        {
            // The lowest 3 bits contain the compression codec
            return (CompressionType)(attributes & 3);
        }

        private static byte[] ReadByteArray(Stream stream)
        {
            var len = BigEndianConverter.ReadInt32(stream);
            if (len == -1)
                return null;
            var buff = new byte[len];
            stream.Read(buff, 0, len);
            return buff;
        }
    }
}
