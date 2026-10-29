using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Benchmarks;

/// <summary>Measures request fingerprint allocation and hashing across stack and pooled payload sizes.</summary>
[MemoryDiagnoser]
public class ReplicaFingerprintBenchmarks
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private NodeCacheEntry<object?> _entry = new(null, 1);

    /// <summary>Gets or sets the request value size.</summary>
    [Params(0, 256, 65536)]
    public int ValueBytes { get; set; }

    /// <summary>Hashes a remove request without a value payload.</summary>
    /// <returns>The request digest.</returns>
    [Benchmark]
    public byte[] Remove() => ReplicaOperationFingerprints.Remove(OperationId, "cache", "key");

    /// <summary>Hashes a set request including its entry.</summary>
    /// <returns>The request digest.</returns>
    [Benchmark]
    public byte[] Set() => ReplicaOperationFingerprints.Set(OperationId, "cache", "key", _entry);

    /// <summary>Creates the value before measurement.</summary>
    [GlobalSetup]
    public void Setup() => _entry = new NodeCacheEntry<object?>(new string('x', ValueBytes), 1);
}
