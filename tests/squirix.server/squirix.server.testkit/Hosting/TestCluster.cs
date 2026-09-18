using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Owns a cluster topology and the in-process nodes started from it.</summary>
/// <typeparam name="TOptions">Node startup options type used by the owning test project.</typeparam>
/// <remarks>
/// The topology belongs to the cluster, so callers start, stop, and restart nodes by identifier and never
/// pass the topology again. The project supplies a starter that starts one topology entry.
/// </remarks>
internal sealed class TestCluster<TOptions> : IAsyncDisposable
    where TOptions : ClusterStartOptions
{
    private readonly TempDirectory? _dataDir;

    /// <summary>TO DO: Field is not (?) used.</summary>
    private readonly ClusterIdentity? _identity;

    private readonly Dictionary<string, ITestNodeHost> _nodes = [with(StringComparer.Ordinal)];
    private readonly Func<ClusterNode, ClusterNode[], TOptions?, CancellationToken, ValueTask<ITestNodeHost>> _startNode;
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="TestCluster{TOptions}" /> class for a single-node topology.</summary>
    /// <param name="node">The only topology entry.</param>
    /// <param name="startNode">Starts the topology entry with the supplied options.</param>
    /// <param name="peers">Optional peer set built from the topology, exposed via <see cref="Peers" />.</param>
    /// <param name="identity">Optional shared mTLS identity disposed with the cluster.</param>
    /// <param name="dataDir">Optional persistence root disposed with the cluster.</param>
    private TestCluster(
        ClusterNode node,
        Func<ClusterNode, ClusterNode[], TOptions?, CancellationToken, ValueTask<ITestNodeHost>> startNode,
        ServerPeer[]? peers = null,
        ClusterIdentity? identity = null,
        TempDirectory? dataDir = null)
        : this([node], startNode, peers, identity, dataDir)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TestCluster{TOptions}" /> class.</summary>
    /// <param name="topology">Cluster members owned by the cluster.</param>
    /// <param name="startNode">Starts one topology entry with the supplied options.</param>
    /// <param name="peers">Optional peer set built from the topology, exposed via <see cref="Peers" />.</param>
    /// <param name="identity">Optional shared mTLS identity disposed with the cluster.</param>
    /// <param name="dataDir">Optional persistence root disposed with the cluster.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> or <paramref name="startNode" /> is <see langword="null" />.</exception>
    private TestCluster(
        ClusterNode[] topology,
        Func<ClusterNode, ClusterNode[], TOptions?, CancellationToken, ValueTask<ITestNodeHost>> startNode,
        ServerPeer[]? peers = null,
        ClusterIdentity? identity = null,
        TempDirectory? dataDir = null)
    {
        ArgumentNullException.ThrowIfNull(topology);
        ArgumentNullException.ThrowIfNull(startNode);
        Topology = topology;
        _startNode = startNode;
        Peers = peers ?? [];
        _identity = identity;
        _dataDir = dataDir;
    }

    /// <summary>Gets the topology owned by the cluster.</summary>
    public ClusterNode[] Topology { get; }

    /// <summary>Gets the peer set built from the topology, or an empty set when none was supplied.</summary>
    public ServerPeer[] Peers { get; }

    /// <summary>Gets the number of nodes that are currently running.</summary>
    public int StartedCount => _nodes.Count;

    /// <summary>Gets the started node with the supplied identifier.</summary>
    /// <param name="nodeId">Node identifier.</param>
    /// <returns>The started test node host.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the node was never started or was already stopped.</exception>
    public ITestNodeHost this[string nodeId] => _nodes.TryGetValue(nodeId, out var node) ? node : throw new KeyNotFoundException($"Cluster node '{nodeId}' is not running.");

    /// <summary>Starts the topology entry with the supplied identifier.</summary>
    /// <param name="nodeId">Node identifier from the cluster topology.</param>
    /// <param name="options">Optional startup options for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started test node host.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="nodeId" /> is not part of the topology.</exception>
    public ValueTask<ITestNodeHost> StartNodeAsync(string nodeId, TOptions? options = null, CancellationToken cancellationToken = default) =>
        StartNodeAsync(FindTopologyNode(nodeId), Topology, options, cancellationToken);

    /// <summary>Starts a node from an explicit topology that may differ from the cluster's own.</summary>
    /// <remarks>
    /// Only for divergence and negative tests: the node identifier may be outside <see cref="Topology" />,
    /// and this start does not update <see cref="Topology" /> or <see cref="Peers" />. A failed start is not
    /// registered, so callers can await it and assert the thrown exception.
    /// </remarks>
    /// <param name="node">Node to start, including its identifier and listen URI.</param>
    /// <param name="topology">Topology presented to the node for this start.</param>
    /// <param name="options">Optional startup options for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started test node host, registered under <paramref name="node" />'s identifier.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> is <see langword="null" />.</exception>
    public async ValueTask<ITestNodeHost> StartNodeAsync(ClusterNode node, ClusterNode[] topology, TOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topology);
        var host = await _startNode(node, topology, options, cancellationToken).ConfigureAwait(false);
        _nodes[node.NodeId] = host;
        return host;
    }

    /// <summary>Stops and removes one node while leaving the rest of the cluster running.</summary>
    /// <param name="nodeId">Node identifier to stop.</param>
    /// <returns>A task that completes after the node stopped.</returns>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "ShutdownAsync performs full teardown; the rule cannot see disposal through ITestNodeHost.")]
    public async ValueTask StopNodeAsync(string nodeId)
    {
        if (_nodes.Remove(nodeId, out var node))
            await node.ShutdownAsync().ConfigureAwait(false);
    }

    /// <summary>Stops the node and starts it again from the same topology entry.</summary>
    /// <param name="nodeId">Node identifier to restart.</param>
    /// <param name="options">Optional startup options for the restarted node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The restarted test node host.</returns>
    public async ValueTask<ITestNodeHost> RestartNodeAsync(string nodeId, TOptions? options = null, CancellationToken cancellationToken = default)
    {
        await StopNodeAsync(nodeId).ConfigureAwait(false);
        return await StartNodeAsync(nodeId, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        foreach (var node in _nodes.Values)
            await node.ShutdownAsync().ConfigureAwait(false);

        _nodes.Clear();
        _identity?.Dispose();
        _dataDir?.Dispose();
    }

    /// <summary>Creates a single-node cluster with in-process nodes.</summary>
    /// <param name="node">The only topology entry.</param>
    /// <param name="peers">Optional peer set built from the topology, exposed via <see cref="Peers" />.</param>
    /// <param name="identity">Optional shared mTLS identity disposed with the cluster.</param>
    /// <param name="dataDir">Optional persistence root disposed with the cluster.</param>
    /// <returns>A cluster that starts in-process nodes.</returns>
    internal static TestCluster<TOptions> Create(ClusterNode node, ServerPeer[]? peers = null, ClusterIdentity? identity = null, TempDirectory? dataDir = null) => new(
        node,
        (self, topology, options, cancellationToken) => StartNodeCoreAsync(self, topology, options, identity, cancellationToken),
        peers,
        identity,
        dataDir);

    /// <summary>Creates a cluster with in-process nodes.</summary>
    /// <param name="topology">Cluster members owned by the cluster.</param>
    /// <param name="peers">Optional peer set built from the topology, exposed via <see cref="Peers" />.</param>
    /// <param name="identity">Optional shared mTLS identity disposed with the cluster.</param>
    /// <param name="dataDir">Optional persistence root disposed with the cluster.</param>
    /// <returns>A cluster that starts in-process nodes.</returns>
    internal static TestCluster<TOptions> Create(ClusterNode[] topology, ServerPeer[]? peers = null, ClusterIdentity? identity = null, TempDirectory? dataDir = null)
    {
        var sharedIdentity = ClusterIdentity.ResolveForTopology(topology, identity);
        return new TestCluster<TOptions>(
            topology,
            (self, entries, options, cancellationToken) => StartNodeCoreAsync(self, entries, options, sharedIdentity, cancellationToken),
            peers,
            sharedIdentity,
            dataDir);
    }

    /// <summary>Creates a cluster from the supplied topology and node starter.</summary>
    /// <param name="topology">Cluster members owned by the cluster.</param>
    /// <param name="startNode">Starts one topology entry with the supplied options.</param>
    /// <param name="peers">Optional peer set built from the topology, exposed via <see cref="Peers" />.</param>
    /// <param name="identity">Optional shared mTLS identity disposed with the cluster.</param>
    /// <param name="dataDir">Optional persistence root disposed with the cluster.</param>
    /// <returns>A cluster that starts nodes through <paramref name="startNode" />.</returns>
    internal static TestCluster<TOptions> Create(
        ClusterNode[] topology,
        Func<ClusterNode, ClusterNode[], TOptions?, CancellationToken, ValueTask<ITestNodeHost>> startNode,
        ServerPeer[]? peers = null,
        ClusterIdentity? identity = null,
        TempDirectory? dataDir = null) => new(topology, startNode, peers, identity, dataDir);

    private ClusterNode FindTopologyNode(string nodeId)
    {
        for (var i = 0; i < Topology.Length; i++)
        {
            if (string.Equals(Topology[i].NodeId, nodeId, StringComparison.Ordinal))
                return Topology[i];
        }

        throw new ArgumentException($"Node '{nodeId}' is not part of the cluster topology.", nameof(nodeId));
    }

    private static NodeHostStartOptions CreateOptions(ClusterStartOptions? co, PersistenceOptions? po, Func<string, HttpMessageHandler>? f, MtlsOptions? mo, MtlsCertificate? mtls)
    {
        return new NodeHostStartOptions
        {
            ConfigureLogging = static b =>
            {
                _ = b.ClearProviders();
                _ = b.SetMinimumLevel(LogLevel.Warning);
                _ = b.AddFilter("Grpc", LogLevel.Warning);
                _ = b.AddFilter("Grpc.AspNetCore.Server", LogLevel.Warning);
                _ = b.AddFilter("Squirix", LogLevel.Warning);
            },
            PersistenceOptions = po,
            PeerHandlerFactory = f,
            SecurityOptions = co?.Security?.ToServerOptions(),
            MtlsOptions = mo,
            MtlsMaterial = mtls,
            TimeProvider = co?.TimeProvider,
        };
    }

    private static async ValueTask<ITestNodeHost> StartNodeCoreAsync(
        ClusterNode self,
        ClusterNode[] topology,
        ClusterStartOptions? options,
        ClusterIdentity? identity,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(topology);

        PersistenceOptions? persistenceOptions = null;
        var dataDir = options?.DataDir;
        if (dataDir != null)
        {
            if (string.IsNullOrWhiteSpace(dataDir))
                throw new ArgumentException("DataDir must be non-empty when persistence is enabled.", nameof(options));

            persistenceOptions = new PersistenceOptions { DataDir = dataDir };
        }

        var callerOwnedIdentity = identity != null;
        try
        {
            var peers = ClusterIdentity.CreatePeers(topology, ref identity);

            var clusterConfig = new TopologyOptions(peers)
            {
                NodeId = self.NodeId,
                Uri = self.Uri,
                VirtualNodes = 128,
                ReplicaCount = options?.ReplicaCount ?? 1,
                ReplicationEnabled = options?.EnableReplication ?? true,
                ConfigurationGeneration = options?.ConfigurationGeneration ?? 1,
            };

            var mtlsProfile = options?.MtlsProfile ?? TestNodeProfile.Normal;
            var (mtlsOptions, material, factory) = identity == null ? new NodeMtlsStartup(null, null, null)
                : await identity.ResolveNodeStartupForBindAsync(clusterConfig, mtlsProfile, cancellationToken).ConfigureAwait(false);

            var nodeHostStartOptions = CreateOptions(options, persistenceOptions, factory, mtlsOptions, material);
            ListenPortPool.ReleaseHeldPrimary(self.Uri);
            var app = await NodeHost.StartAsync(clusterConfig, nodeHostStartOptions, cancellationToken).ConfigureAwait(false);

            return new TestNodeHost(app, self.Uri, persistenceOptions?.DataDir ?? string.Empty, persistenceOptions != null, callerOwnedIdentity ? null : identity);
        }
        catch
        {
            if (!callerOwnedIdentity)
                identity?.Dispose();
            ListenPortPool.ReleaseHeldPrimary(self.Uri);
            throw;
        }
    }
}
