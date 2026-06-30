using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using Squirix.Benchmarks.Fixtures;
using Squirix.Internal.Cluster.Transport.Binary;
using Squirix.Serialization;

namespace Squirix.Benchmarks.Cache;

/// <summary>
/// In-process allocation baselines for structured wire encode/decode without gRPC.
/// Compare with E2E <c>CacheWireStructuredAllocBenchmarks</c> to isolate codec vs transport overhead.
/// </summary>
[MemoryDiagnoser]
[MinIterationTime(150)]
public class MetadataWireCodecAllocBenchmarks
{
    private const int Batch = 512;

    private readonly Consumer _consumer = new();
    private readonly ISquirixSerializer _serializer = new SystemTextJsonSerializer();

    private byte[] _wire = [];
    private WireStructuredProfile? _profile;

    /// <summary>Encodes a structured profile to owned wire bytes.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void EncodeStructuredBatched()
    {
        var profile = _profile!;
        for (var i = 0; i < Batch; i++)
        {
            var bytes = CacheValueWireCodec.EncodeWireValueToOwned(profile, _serializer);
            _consumer.Consume(bytes.Length);
        }
    }

    /// <summary>Decodes structured profile bytes through metadata codec.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void DecodeStructuredBatched()
    {
        var wire = _wire;
        for (var i = 0; i < Batch; i++)
        {
            _ = CacheValueWireCodec.TryReadWireValue(wire, _serializer, out WireStructuredProfile? decoded);
            _consumer.Consume(decoded?.Id ?? 0);
        }
    }

    /// <summary>Full metadata encode then decode for one profile.</summary>
    [Benchmark(OperationsPerInvoke = Batch)]
    public void RoundTripStructuredBatched()
    {
        var profile = _profile!;
        for (var i = 0; i < Batch; i++)
        {
            var bytes = CacheValueWireCodec.EncodeWireValueToOwned(profile, _serializer);
            _ = CacheValueWireCodec.TryReadWireValue(bytes, _serializer, out WireStructuredProfile? decoded);
            _consumer.Consume(decoded?.Id ?? 0);
        }
    }

    /// <summary>Prepares profile fixture and one encoded payload for decode benchmarks.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _profile = WireStructuredProfile.Create(0);
        _wire = CacheValueWireCodec.EncodeWireValueToOwned(_profile, _serializer);
    }
}
