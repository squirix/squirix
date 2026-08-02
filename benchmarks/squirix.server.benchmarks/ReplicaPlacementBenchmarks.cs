using System;
using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.Benchmarks;

/// <summary>Placement and topology fingerprint hot-path benchmarks.</summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 2, iterationCount: 5)]
[SuppressMessage("ReSharper", "UnusedMember.Global", Justification = "BenchmarkDotNet discovers benchmark methods by reflection.")]
public class ReplicaPlacementBenchmarks
{
    private string[] _groupBuffer = [];
    private INodeLocator? _locator;
    private MtlsOptions? _mtls;
    private IReplicaGroupLocator? _rf1;
    private IReplicaGroupLocator? _rf2;
    private IReplicaGroupLocator? _rf3;
    private IReplicaGroupLocator? _rf5;
    private TopologyOptions? _topology;

    /// <summary>Computes the topology fingerprint for a fixed five-node vector.</summary>
    /// <returns>Fingerprint hex length used as a sink.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup has not run.</exception>
    [Benchmark]
    public int ComputeTopologyFingerprint()
    {
        var topology = _topology ?? throw new InvalidOperationException("Benchmark was not initialized.");
        var mtls = _mtls ?? throw new InvalidOperationException("Benchmark was not initialized.");
        return topology.CreateFingerprint(mtls).ToString().Length;
    }

    /// <summary>Resolves the original owner for a fixed cache key.</summary>
    /// <returns>Owner node identifier.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup has not run.</exception>
    [Benchmark]
    public string GetOriginalOwner()
    {
        var locator = _locator ?? throw new InvalidOperationException("Benchmark was not initialized.");
        return locator.GetOwner("cache", "bench-key");
    }

    /// <summary>Resolves an RF=1 replica group without allocating.</summary>
    /// <returns>Original owner from the group.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup has not run.</exception>
    [Benchmark]
    public string GetReplicaGroupRfOne() => WriteGroup(_rf1);

    /// <summary>Resolves an RF=2 replica group without allocating.</summary>
    /// <returns>Original owner from the group.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup has not run.</exception>
    [Benchmark]
    public string GetReplicaGroupRfTwo() => WriteGroup(_rf2);

    /// <summary>Resolves an RF=3 replica group without allocating.</summary>
    /// <returns>Original owner from the group.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup has not run.</exception>
    [Benchmark]
    public string GetReplicaGroupRfThree() => WriteGroup(_rf3);

    /// <summary>Resolves an RF=5 replica group without allocating.</summary>
    /// <returns>Original owner from the group.</returns>
    /// <exception cref="InvalidOperationException">Thrown when setup has not run.</exception>
    [Benchmark]
    public string GetReplicaGroupRfFive() => WriteGroup(_rf5);

    /// <summary>Builds locators used by ownership and replica-group benchmarks.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var topology = CreateTopology();
        var nodes = new string[topology.Peers.Length];
        for (var i = 0; i < nodes.Length; i++)
            nodes[i] = topology.Peers[i].NodeId;

        var ring = new PhysicalNodeRing(nodes);
        _locator = RuntimeServiceRegistration.CreateHashLocator(nodes, topology.VirtualNodes);
        _rf1 = new ReplicaGroupLocator(ring, 1);
        _rf2 = new ReplicaGroupLocator(ring, 2);
        _rf3 = new ReplicaGroupLocator(ring, 3);
        _rf5 = new ReplicaGroupLocator(ring, 5);
        _groupBuffer = new string[PolicyOptions.MaxReplicaCount];
        _topology = topology;
        _mtls = new MtlsOptions();
    }

    private static TopologyOptions CreateTopology()
    {
        ServerPeer[] peers =
        [
            new() { NodeId = "node-a", Uri = new Uri("https://127.0.0.1:6001") },
            new() { NodeId = "node-b", Uri = new Uri("https://127.0.0.1:6002") },
            new() { NodeId = "node-c", Uri = new Uri("https://127.0.0.1:6003") },
            new() { NodeId = "node-d", Uri = new Uri("https://127.0.0.1:6004") },
            new() { NodeId = "node-e", Uri = new Uri("https://127.0.0.1:6005") },
        ];
        return new TopologyOptions(peers)
        {
            ClusterId = "bench",
            NodeId = "node-a",
            Uri = peers[0].Uri,
            VirtualNodes = 128,
            ReplicaCount = 3,
            ConfigurationGeneration = 1,
        };
    }

    private string WriteGroup(IReplicaGroupLocator? locator)
    {
        var groupLocator = locator ?? throw new InvalidOperationException("Benchmark was not initialized.");
        var destination = _groupBuffer.AsSpan(0, groupLocator.ReplicaCount);
        groupLocator.GetReplicaGroup("node-c", destination);
        return destination[0];
    }
}
