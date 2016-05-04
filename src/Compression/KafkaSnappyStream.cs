using System;
using System.IO;
using System.Text;
using kafka4net.Compression;
using kafka4net.Utils;

namespace kafka4net.Protocols
{
    /// <summary>
    /// Kafka's snappy framing is non-standard. See https://github.com/xerial/snappy-java
    /// [magic header:16 bytes]([block size:int32][compressed data:byte array])*
    /// </summary>
    class KafkaSnappyStream : Stream
    {
        static readonly byte[] _snappyMagic = { 0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0 };
        const int SnappyHeaderLen = 16; // 8 bytes of magic and 2 ints for version compatibility

        CompressionStreamMode _mode;
        readonly Stream _base;
        byte[] _uncompressedBuffer = new byte[32*1024];  // 32K is default buffer size in xerces
        // Working buffer to store data from compressed stream and avoid allocations. Can be resized
        byte[] _compressedBuffer = new byte[10*1024];
        int _bufferLen;
        int _bufferPtr;
        readonly byte[] _headerWorkingBuffer = new byte[SnappyHeaderLen];

        public KafkaSnappyStream(Stream stream, CompressionStreamMode mode)
        {
            _mode = mode;
            _base = stream;
            var hasHeader = ReadHeader();

            if (!hasHeader)
                throw new InvalidDataException("Failed to read snappy header");
            else
            {
                ReadBlock();
            }

        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if(_bufferPtr < _bufferLen)
                return ReadInternalBuffer(buffer, offset, count);

            if (!ReadBlock())
                return 0;
            else
                return ReadInternalBuffer(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override bool CanRead => _mode == CompressionStreamMode.Decompress;
        public override bool CanSeek => false;
        public override bool CanWrite => _mode == CompressionStreamMode.Compress;
        public override long Length { get { throw new NotImplementedException(); } }
        public override long Position { get { throw new NotImplementedException(); } set { throw new NotImplementedException(); } }

        //
        // Implementation
        //

        bool ReadHeader()
        {
            //
            // Validate snappy header
            //
            if (!StreamUtils.ReadAll(_base, _headerWorkingBuffer, SnappyHeaderLen))
                return false;

            for(int i=0; i<_snappyMagic.Length; i++)
                if(_headerWorkingBuffer[i] != _snappyMagic[i])
                    throw new InvalidDataException("Invalid snappy magic bytes");

            // TODO: validate snappy min compatible version

            return true;
        }

        bool ReadBlock()
        {
            if (!StreamUtils.ReadAll(_base, _compressedBuffer, 4))
                return false;

            var blockSize = BigEndianConverter.ReadInt32(_compressedBuffer);

            if (blockSize > _compressedBuffer.Length)
                _compressedBuffer = new byte[blockSize];

            if(!StreamUtils.ReadAll(_base, _compressedBuffer, blockSize))
                throw new InvalidDataException("Unexpected end of snappy data block");

            // Reallocate compressed buffer if needed
            var uncompressedLen = Snappy.SnappyCodec.GetUncompressedLength(_compressedBuffer, 0, blockSize);
            if (uncompressedLen > _uncompressedBuffer.Length)
                _uncompressedBuffer = new byte[uncompressedLen];

            var read = Snappy.SnappyCodec.Uncompress(_compressedBuffer, 0, blockSize, _uncompressedBuffer, 0);
            
            _bufferLen = read;
            _bufferPtr = 0;

            return true;
        }

        int ReadInternalBuffer(byte[] buffer, int offset, int count)
        {
            var canRead = Math.Min(count, _bufferLen - _bufferPtr);
            Buffer.BlockCopy(_uncompressedBuffer, _bufferPtr, buffer, offset, canRead);
            _bufferPtr += canRead;
            return canRead;
        }
    }

    enum CompressionStreamMode
    {
        Compress,
        Decompress
    }
}
