using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>CRC32C checksum throughput benchmarks.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 1, iterationCount: 3)]
public class Crc32CBenchmarks
{
    private byte[] _buffer = [];

    /// <summary>Gets or sets the number of bytes to checksum.</summary>
    [Params(64, 1024, 65536)]
    public int Size { get; set; }

    /// <summary>Computes the checksum of a random buffer.</summary>
    /// <returns>The checksum.</returns>
    [Benchmark]
    public uint Compute() => Crc32C.Compute(_buffer);

    /// <summary>Fills the buffer with random bytes.</summary>
    [GlobalSetup]
    public void GlobalSetup()
    {
        _buffer = new byte[Size];
        RandomNumberGenerator.Fill(_buffer);
    }
}
