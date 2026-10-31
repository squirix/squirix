using BenchmarkDotNet.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Benchmarks;

/// <summary>Serializing empty and nonempty mutation outcomes into owned replay bytes.</summary>
[MemoryDiagnoser]
public class IdempotencyResponseBufferBenchmarks
{
    private readonly SetAsyncResponse _empty = new();
    private readonly TouchAsyncResponse _nonempty = new() { Found = true };

    /// <summary>Serializes an empty Set outcome.</summary>
    /// <returns>The replay bytes.</returns>
    [Benchmark]
    public byte[] Empty() => IdempotencyResponseCodec.SerializeResponseBytes(_empty);

    /// <summary>Serializes a nonempty Touch outcome.</summary>
    /// <returns>The replay bytes.</returns>
    [Benchmark]
    public byte[] Nonempty() => IdempotencyResponseCodec.SerializeResponseBytes(_nonempty);
}
