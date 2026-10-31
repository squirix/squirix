using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of encoding one replica log record to its canonical bytes, paid by the leader per mutation and by the follower per received record.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class ReplicaLogCodecEncodeBenchmarks
{
    private const int FingerprintSize = 16;
    private const int LargeMutationSize = 4096;

    private ReplicaLogRecord _large;
    private ReplicaLogRecord _typical;

    /// <summary>Creates a typical record and one with a large mutation payload.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _typical = CreateRecord(32);
        _large = CreateRecord(LargeMutationSize);
    }

    /// <summary>Encodes a record with short identifiers and small payloads.</summary>
    /// <returns>The canonical bytes.</returns>
    [Benchmark(Baseline = true)]
    public byte[] EncodeTypical() => ReplicaLogCodec.Encode(in _typical);

    /// <summary>Encodes a record with a 4 KB mutation payload.</summary>
    /// <returns>The canonical bytes.</returns>
    [Benchmark]
    public byte[] EncodeLargeMutation() => ReplicaLogCodec.Encode(in _large);

    private static ReplicaLogRecord CreateRecord(int mutationSize) => new(
        42,
        3,
        "5d0c2f1e7a3b4c9d8e6f1a2b3c4d5e6f",
        "user",
        new byte[FingerprintSize],
        "mutation",
        "orders",
        new byte[24],
        "Set",
        new byte[mutationSize],
        new byte[16],
        638000000000000000,
        638000000000000000,
        638000000000000000);
}
