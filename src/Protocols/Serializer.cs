using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using kafka4net.Metadata;
using kafka4net.Protocols.Requests;
using kafka4net.Protocols.Responses;
using kafka4net.Tracing;
using kafka4net.Utils;

namespace kafka4net.Protocols
{
    // TODO: move functions into business objects
    static class Serializer
    {
        static readonly byte[] _minusOne32 = { 0xff, 0xff, 0xff, 0xff };
        static readonly byte[] _one32 = { 0x00, 0x00, 0x00, 0x01 };
        static readonly byte[] _two32 = { 0x00, 0x00, 0x00, 0x02 };
        static readonly byte[] _eight32 = { 0x00, 0x00, 0x00, 0x08 };
        static readonly byte[] _minusOne16 = { 0xff, 0xff };
        static readonly byte[] _apiVersion = { 0x00, 0x00 };
        static readonly byte[] _zero64 = { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };
        static readonly byte[] _zero32 = { 0x00, 0x00, 0x00, 0x00 };
        // TODO: make it configurable
        static byte[] _clientId;

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

        public static MetadataResponse DeserializeMetadataResponse(byte[] body)
        {
            var stream = new MemoryStream(body);
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
            ret.TopicErrorCode = (ErrorCode)BigEndianConverter.ReadInt16(stream);
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
            Write(stream, partition.Messages);
        }

        private static void Write(MemoryStream stream, IEnumerable<MessageData> messages)
        {
            // MessageSetInBytes
            WriteSizeInBytes(stream, () =>
            {
                foreach (var message in messages)
                {
                    stream.Write(_zero64, 0, 8); // producer does fake offset
                    var m = message;
                    WriteSizeInBytes(stream, () => Write(stream, m));
                }
            });
        }

        private static void Write(MemoryStream stream, MessageData message)
        {
            var crcPos = stream.Position;
            stream.Write(_minusOne32, 0, 4); // crc placeholder
            var bodyPos = stream.Position;
            
            stream.WriteByte(0); // magic byte
            stream.WriteByte(0); // attributes
            if (message.Key == null)
            {
                stream.Write(_minusOne32, 0, 4);
            }
            else
            {
                BigEndianConverter.Write(stream, message.Key.Length);
                stream.Write(message.Key, 0, message.Key.Length);
            }
            BigEndianConverter.Write(stream, message.Value.Length);
            stream.Write(message.Value, 0, message.Value.Length);

            // update crc
            var crc = Crc32.Compute(stream, bodyPos, stream.Position - bodyPos);
            Update(stream, crcPos, crc);
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

        static void WriteSizeInBytes(MemoryStream stream, Action write)
        {
            stream.Write(_zero32, 0, 4);
            var initPos = stream.Position;
            write();
            var pos = stream.Position;
            var size = pos - initPos;
            stream.Position = initPos - 4;
            BigEndianConverter.Write(stream, (int)size);
            stream.Position = pos;
        }

        static void Update(MemoryStream stream, long pos, byte[] buff)
        {
            var currPos = stream.Position;
            stream.Position = pos;
            stream.Write(buff, 0, buff.Length);
            stream.Position = currPos;
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

        public static ProducerResponse GetProducerResponse(byte[] buff)
        {
            var resp = new ProducerResponse();
            var stream = new MemoryStream(buff);
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

        internal static OffsetResponse DeserializeOffsetResponse(byte[] buff)
        {
            var stream = new MemoryStream(buff);
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
        public static FetchResponse DeserializeFetchResponse(byte[] buff)
        {
            var stream = new MemoryStream(buff);
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
                    topic.Partitions[i] = new FetchResponse.PartitionFetchData
                    {
                        Partition = BigEndianConverter.ReadInt32(stream),
                        ErrorCode = (ErrorCode)BigEndianConverter.ReadInt16(stream),
                        HighWatermarkOffset = BigEndianConverter.ReadInt64(stream),
                        Messages = ReadMessageSet(stream).ToArray()
                    };
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
        private static IEnumerable<Message> ReadMessageSet(MemoryStream stream)
        {
            // "As an optimization the server is allowed to return a partial message at the end of the message set. 
            // Clients should handle this case"

            var messageSetSize = BigEndianConverter.ReadInt32(stream);
            var remainingMessageSetBytes = messageSetSize;

            while (remainingMessageSetBytes > 0)
            {
                // we need at least be able to read offset and messageSize
                if (remainingMessageSetBytes < + 8 + 4)
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
                var crc = BigEndianConverter.ReadInt32(stream);
                var crcPos = stream.Position;
                var magic = stream.ReadByte();
                var attributes = stream.ReadByte();
                var msg = new Message();
                msg.Key = ReadByteArray(stream);
                msg.Value = ReadByteArray(stream);
                msg.Offset = offset;
                var pos = stream.Position;
                var computedCrcArray = Crc32.Compute(stream, crcPos, pos - crcPos);
                var computedCrc = BigEndianConverter.ToInt32(computedCrcArray);
                if (computedCrc != crc)
                {
                    throw new BrokerException(string.Format("Corrupt message: Crc does not match. Caclulated {0} but got {1}", computedCrc, crc));
                }
                yield return msg;

                // subtract messageSize of that message from remaining bytes
                remainingMessageSetBytes -= messageSize;
            }
        }

        private static byte[] ReadByteArray(MemoryStream stream)
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
