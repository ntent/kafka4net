using System;
using System.IO;
using kafka4net.Utils;
using Snappy;

namespace kafka4net.Compression
{
    /// <summary>
    /// Kafka's snappy framing is non-standard. See https://github.com/xerial/snappy-java
    /// [magic header:16 bytes]([block size:int32][compressed data:byte array])*
    /// Version and min compatible version are both 1
    /// </summary>
    sealed class KafkaSnappyStream : Stream
    {
        static readonly byte[] _snappyMagic = { 0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0 };
        const int SnappyHeaderLen = 16; // 8 bytes of magic and 2 ints for version compatibility

        CompressionStreamMode _mode;
        readonly Stream _base;
        byte[] _uncompressedBuffer;
        // Working buffer to store data from compressed stream and avoid allocations. Can be resized
        byte[] _compressedBuffer;// = new byte[10*1024];
        int _bufferLen;
        int _bufferPtr;
        readonly byte[] _headerWorkingBuffer = new byte[SnappyHeaderLen];
        static readonly byte[] _versionsHeader = { 0, 0, 0, 1, 0, 0, 0, 1};

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="mode"></param>
        /// <param name="uncompressedBuffer">Recommended size is 32Kb, as default in java xerces implementation</param>
        public KafkaSnappyStream(Stream stream, CompressionStreamMode mode, byte[] uncompressedBuffer, byte[] compressedBuffer)
        {
            _mode = mode;
            _base = stream;
            _uncompressedBuffer = uncompressedBuffer;
            _compressedBuffer = compressedBuffer;

            if (mode == CompressionStreamMode.Decompress)
            {
                if (!ReadHeader())
                    throw new InvalidDataException("Failed to read snappy header");
                ReadBlock();
            }
            else
            {
                WriteHeader();
            }
        }

        /// <summary>
        /// Allocate buffers of size, recommended for kafka usage. CompressedBuffer will be allocated of enough size, which will not require reallocation.
        /// </summary>
        /// <param name="uncompressedBuffer"></param>
        /// <param name="compressedBuffer"></param>
        public static void AllocateBuffers(out byte[] uncompressedBuffer, out byte[] compressedBuffer)
        {
            uncompressedBuffer = new byte[32 * 1024];  // 32K is default buffer size in xerces
            compressedBuffer = new byte[SnappyCodec.GetMaxCompressedLength(uncompressedBuffer.Length)];
        }

        public override void Flush()
        {
            if(_bufferLen == 0)
                return;

            Flush(_uncompressedBuffer, 0, _bufferLen);
            _bufferLen = 0;
        }

        void Flush(byte[] buff, int offset, int count)
        {
            var compressedSize = SnappyCodec.Compress(buff, offset, count, _compressedBuffer, 0);

            BigEndianConverter.Write(_base, compressedSize);
            _base.Write(_compressedBuffer, 0, compressedSize);
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
            if(!CanRead)
                throw new InvalidOperationException("Not a read stream");

            if(_bufferPtr < _bufferLen)
                return ReadInternalBuffer(buffer, offset, count);

            if (!ReadBlock())
                return 0;
            else
                return ReadInternalBuffer(buffer, offset, count);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if(!CanWrite)
                throw new InvalidOperationException("Not a write stream");

            while(count > 0)
            {
                var size = Math.Min(count, _uncompressedBuffer.Length - _bufferLen);

                // Optimization: if full frame is going to be flushed, skip array copying and compress stright from input buffer
                if (_bufferLen == 0 && size == _uncompressedBuffer.Length)
                {
                    Flush(buffer, offset, size);
                    count -= size;
                    offset += size;
                    continue;
                }

                Buffer.BlockCopy(buffer, offset, _uncompressedBuffer, _bufferLen, size);
                _bufferLen += size;
                count -= size;
                offset += size;

                if(_bufferLen == _uncompressedBuffer.Length)
                    Flush();
            }
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

        void WriteHeader()
        {
            _base.Write(_snappyMagic, 0, _snappyMagic.Length);
            _base.Write(_versionsHeader, 0, 8);
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

            var read = SnappyCodec.Uncompress(_compressedBuffer, 0, blockSize, _uncompressedBuffer, 0);
            
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

        protected override void Dispose(bool disposing)
        {
            if(_mode == CompressionStreamMode.Compress && _bufferLen != 0)
                Flush();

            base.Dispose(disposing);
        }
    }

    enum CompressionStreamMode
    {
        Compress,
        Decompress
    }
}
