using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Benchmarks;

/// <summary>Allocations and elapsed time of decoding one canonical record during replica apply.</summary>
[MemoryDiagnoser]
public class ReplicaLogDecodeBenchmarks
{
    private byte[] _payload = [];

    /// <summary>Gets or sets the size of the mutation payload; other fields stay fixed.</summary>
    [Params(0, 32, 4096, 65536)]
    public int PayloadBytes { get; set; }

    /// <summary>Decodes a record whose output owns its byte fields independently of the input.</summary>
    /// <returns>The decoded log index, so the result is consumed without boxing the record.</returns>
    [Benchmark]
    public ulong Decode() => ReplicaLogCodec.Decode(_payload)?.LogIndex ?? 0UL;

    /// <summary>Encodes the input once, outside the measured decode.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var record = new ReplicaLogRecord(
            42,
            3,
            "5d0c2f1e7a3b4c9d8e6f1a2b3c4d5e6f",
            "user",
            new byte[16],
            "mutation",
            "orders",
            new byte[24],
            "Set",
            new byte[PayloadBytes],
            new byte[16],
            638000000000000000,
            638000000000000000,
            638000000000000000);
        _payload = ReplicaLogCodec.Encode(in record);
    }
}
