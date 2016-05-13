using System;
using System.IO;
using System.Linq;
using kafka4net.Compression;
using NUnit.Framework;

namespace tests
{
    [TestFixture]
    public class CompressionTests
    {
        [Test]
        [Category("Compression")]
        public void TestLz4Stream()
        {
            var buffers = Enumerable.Range(1, 256 * 1024).
                AsParallel().
                Select(size => {
                    var buff = new byte[size];
                    new Random().NextBytes(buff);
                    return buff;
            });

            buffers.
                ForAll(buffer =>
                {
                    var compressed = new MemoryStream();
                    var lz = new Lz4KafkaStream(compressed, CompressionStreamMode.Compress);
                    lz.Write(buffer, 0, buffer.Length);
                    lz.Close();
                    var compressedBuff = compressed.ToArray();

                    var lz2 = new Lz4KafkaStream(new MemoryStream(compressedBuff), CompressionStreamMode.Decompress);
                    var uncompressed = new MemoryStream();
                    lz2.CopyTo(uncompressed);
                    var uncompressedBuffer = uncompressed.ToArray();

                    var res = buffer.SequenceEqual(uncompressedBuffer);
                    Assert.IsTrue(res, $"Buffer size {buffer.Length}");
                });
        }
    }
}
