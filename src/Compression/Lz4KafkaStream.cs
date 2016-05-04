using System;
using System.IO;
using kafka4net.Compression;
using kafka4net.Utils;
using LZ4;
using xxHashSharp;

namespace kafka4net.Protocols
{
    class Lz4KafkaStream : Stream
    {
        Stream _base;
        CompressionStreamMode _mode;
        readonly byte[] _headerBuffer = new byte[LZ4_MAX_HEADER_LENGTH];
        byte[] _blockBuffer;
        byte[] _uncompressedBuffer;
        static readonly int[] _maxBlockSizeTable = new []{0,0,0,0, 64*1024, 256*1024, 1024*1024, 4*1024*1024 };

        const int LZ4_MAX_HEADER_LENGTH = 19;
        const uint MAGIC = 0x184D2204;
        xxHash _hasher;
        int _bufferLen;
        int _bufferPtr;
        bool _isComplete;

        public Lz4KafkaStream(Stream @base, CompressionStreamMode mode)
        {
            _base = @base;
            _mode = mode;
            _hasher = new xxHash();
            if (!ReadHeader())
                throw new InvalidDataException("Failed to read lz4 header");

            ReadBlock();
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
            if (_bufferPtr < _bufferLen)
                return ReadInternalBuffer(buffer, offset, count);

            if (_isComplete)
                return 0;

            if (!ReadBlock())
                return 0;

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
            // Read magic, FLG/BD and Descriptor Checksum
            if (!StreamUtils.ReadAll(_base, _headerBuffer, 7))
                return false;

            if(LittleEndianConverter.ReadUInt32(_headerBuffer, 0) != MAGIC)
                throw new InvalidDataException("Invalid lz4 magic");

            // parse FLG
            var flg = _headerBuffer[4];
            var version = flg >> 6;
            if (version != 1)
                throw new InvalidDataException($"Unsupported version of LZ4 format. Supported 1 but got {version}");

            var hasBlockChecksum = (flg & (1 << 4)) != 0;
            if(hasBlockChecksum)
                throw new NotImplementedException("Block checksum is not implemented");


            // parse BD and allocate uncompressed buffer
            var bd = _headerBuffer[5];
            var maxBlockSizeIndex = (bd >> 4) & 0x7;
            if(maxBlockSizeIndex >= _maxBlockSizeTable.Length)
                throw new InvalidDataException($"Invalid LZ4 max data block size index: {maxBlockSizeIndex}");
            int maxBlockSize = _maxBlockSizeTable[maxBlockSizeIndex];
            if(maxBlockSize == 0)
                throw new InvalidDataException($"Invalid LZ4 max data block size index: {maxBlockSizeIndex}");
            _uncompressedBuffer = new byte[maxBlockSize];

            _hasher.Init();
            // Yep, this is the bug in kafka's framing checksum KAFKA-3160. Magic should not be checksummed but it is
            _hasher.Update(_headerBuffer, 6);   // Will need to patch it to accept offset in order to avoid unneeded reallocations, 
                                                // when want to exlude magic
            var calculatedChecksum = (_hasher.Digest() >> 8) & 0xff;

            if(calculatedChecksum != _headerBuffer[6])
                throw new InvalidDataException("Lz4 Frame Descriptor checksum mismatch");

            return true;
        }

        bool ReadBlock()
        {
            // Reuse _uncompressedBuffer to read block size uint32
            if (!StreamUtils.ReadAll(_base, _uncompressedBuffer, 4))
                throw new InvalidDataException("Unexpected end of LZ4 data block");
            var blockSize = (int)LittleEndianConverter.ReadUInt32(_uncompressedBuffer, 0);
            if (blockSize == 0)
            {
                _isComplete = true;
                return false;
            }

            var isCompressed = (blockSize & 0x80000000) == 0;
            blockSize = blockSize & 0x7fffffff;

            if (_blockBuffer == null || _blockBuffer.Length < blockSize)
                _blockBuffer = new byte[Math.Max(blockSize, 16*1024)];

            if(!StreamUtils.ReadAll(_base, _blockBuffer, blockSize))
                throw new InvalidDataException("Unexpected end of LZ4 data block");

            // Ignore block checksum because kafka does not set it

            if (!isCompressed)
            {
                _blockBuffer.CopyTo(_uncompressedBuffer, 0);
                _bufferLen = blockSize;
                _bufferPtr = 0;
                return true;
            }


            var decodedSize = LZ4Codec.Decode(_blockBuffer, 0, blockSize, _uncompressedBuffer, 0, _uncompressedBuffer.Length);

            _bufferLen = decodedSize;
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
}
