using System;
using System.Security.Cryptography.X509Certificates;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of checking a peer certificate during a handshake: a fresh chain build against a cache hit.</summary>
[MemoryDiagnoser]
public class PeerChainCacheBenchmarks
{
    private const string PeerNodeId = "bench-node-b";
    private X509Certificate2? _anchor;
    private PeerChainCache? _cache;
    private X509Certificate2? _peer;

    /// <summary>Releases the certificates.</summary>
    [GlobalCleanup]
    public void Cleanup()
    {
        _peer?.Dispose();
        _anchor?.Dispose();
    }

    /// <summary>Creates a test authority and a peer certificate, and warms the cache with that certificate.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _anchor = TestCertificates.CreateStandaloneCertificateAuthority();
        _peer = TestCertificates.CreatePeerCertificate(_anchor, PeerNodeId);
        _cache = new PeerChainCache(_anchor, PeerChainCache.DefaultCapacity, PeerChainCache.DefaultMaxAge);
        _ = _cache.TryVerify(_peer, DateTimeOffset.UtcNow, out _);
    }

    /// <summary>Builds the chain on every call, as each handshake did before the cache.</summary>
    /// <returns>Whether the certificate is trusted.</returns>
    [Benchmark(Baseline = true, Description = "Chain build on every check")]
    public bool ChainBuild()
    {
        var cache = new PeerChainCache(_anchor!, 1, TimeSpan.Zero);
        return cache.TryVerify(_peer!, DateTimeOffset.UtcNow, out _);
    }

    /// <summary>Answers from the cache.</summary>
    /// <returns>Whether the certificate is trusted.</returns>
    [Benchmark(Description = "Cache hit")]
    public bool CacheHit() => _cache!.TryVerify(_peer!, DateTimeOffset.UtcNow, out _);
}
