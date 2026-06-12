using System;
using System.Collections.Generic;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Shared cluster CA and per-node mTLS material for multi-node test hosts in one test case.
/// </summary>
public sealed class MtlsTestContext : IDisposable
{
    private readonly Dictionary<string, int> _internalPortsByNodeId = new(StringComparer.Ordinal);
    private MtlsTestBundle? _bundle;

    /// <inheritdoc />
    public void Dispose()
    {
        _bundle?.Dispose();
        _bundle = null;
        _internalPortsByNodeId.Clear();
    }

    /// <summary>
    /// Builds peer entries for a multi-node topology, including dedicated inter-node URLs.
    /// </summary>
    /// <param name="shared">Shared context for the current test case.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>Peer entries for host startup.</returns>
    internal static Peer[] CreatePeers(ref MtlsTestContext? shared, IReadOnlyList<(string NodeId, string Url)> topology)
    {
        ArgumentNullException.ThrowIfNull(topology);

        if (!HasRemotePeers(topology))
        {
            var standalonePeers = new Peer[topology.Count];
            for (var i = 0; i < topology.Count; i++)
                standalonePeers[i] = new Peer { NodeId = topology[i].NodeId, Url = topology[i].Url };

            return standalonePeers;
        }

        shared ??= new MtlsTestContext();
        return shared.BuildPeers(topology);
    }

    /// <summary>
    /// Resolves startup overrides for a node, reusing one shared context per test case.
    /// </summary>
    /// <param name="shared">Shared context for the current test case.</param>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <returns>Startup overrides, or <c>null</c> values for standalone topology.</returns>
    internal static (MtlsOptions? Options, MtlsCertificateMaterial? Material) ResolveForNode(ref MtlsTestContext? shared, ClusterConfig cluster, string url)
    {
        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null);

        shared ??= new MtlsTestContext();
        return shared.Resolve(cluster, url);
    }

    /// <summary>
    /// Creates cluster mTLS startup overrides for the node being started.
    /// </summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <returns>Options and material for host startup overrides.</returns>
    internal (MtlsOptions? Options, MtlsCertificateMaterial? Material) Resolve(ClusterConfig cluster, string url)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentException.ThrowIfNullOrWhiteSpace(url);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null);

        _bundle ??= new MtlsTestBundle();
        var primaryPort = new Uri(url).Port;
        var internalPort = GetOrAllocateInternalPort(cluster.NodeId, url);
        return _bundle.CreateNode(cluster.NodeId, primaryPort, internalPort);
    }

    private static string CreateInterNodeUrl(string primaryUrl, int internalPort)
    {
        var primaryUri = new Uri(primaryUrl);
        return new UriBuilder(primaryUri.Scheme, primaryUri.Host, internalPort).Uri.AbsoluteUri;
    }

    private static bool HasRemotePeers(IReadOnlyList<(string NodeId, string Url)> topology)
    {
        var nodeIds = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < topology.Count; i++)
            _ = nodeIds.Add(topology[i].NodeId);

        return nodeIds.Count > 1;
    }

    private Peer[] BuildPeers(IReadOnlyList<(string NodeId, string Url)> topology)
    {
        var peers = new Peer[topology.Count];
        for (var i = 0; i < topology.Count; i++)
        {
            var (nodeId, url) = topology[i];
            var internalPort = GetOrAllocateInternalPort(nodeId, url);
            peers[i] = new Peer
            {
                NodeId = nodeId,
                Url = url,
                InterNodeUrl = CreateInterNodeUrl(url, internalPort),
            };
        }

        return peers;
    }

    private int GetOrAllocateInternalPort(string nodeId, string url)
    {
        if (_internalPortsByNodeId.TryGetValue(nodeId, out var existingPort))
            return existingPort;

        var primaryPort = new Uri(url).Port;
        var internalPort = MtlsTestPorts.AllocateInternalPort(primaryPort);
        _internalPortsByNodeId[nodeId] = internalPort;
        return internalPort;
    }
}
