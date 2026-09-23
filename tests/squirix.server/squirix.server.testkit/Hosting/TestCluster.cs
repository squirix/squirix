using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Owns a cluster topology and the in-process nodes started from it.</summary>
/// <typeparam name="TOptions">Node startup options type used by the owning test project.</typeparam>
/// <remarks>
/// The topology belongs to the cluster, so callers start, stop, and restart nodes by identifier and never
/// pass the topology again. The project supplies a starter that starts one topology entry.
/// <see cref="DisposeAsync" /> waits for every <see cref="StopNodeAsync" /> call already in flight when it starts
/// before releasing the shared identity and data directory. A <see cref="StopNodeAsync" /> call that begins after
/// <see cref="DisposeAsync" /> has started tearing the cluster down is not linearized against it and should be
/// avoided: dispose the cluster only once every intended <see cref="StopNodeAsync" /> and
/// <see cref="RestartNodeAsync" /> call for the test has returned. Concurrent <see cref="StartNodeAsync(string, TOptions, CancellationToken)" />
/// calls for distinct node identifiers are not: when the topology requires internode mTLS, every node shares
/// one unsynchronized <c language="csharp">ClusterIdentity</c>, so starting more than one node at a time races
/// on its port and certificate bookkeeping. Start nodes for a shared-identity topology sequentially (for
/// example through <see cref="StartAllAsync" />).
/// </remarks>
internal sealed class TestCluster<TOptions> : IAsyncDisposable
    where TOptions : ClusterStartOptions
{
    /// <summary>Shared mTLS identity disposed with the cluster.</summary>
    private readonly ClusterIdentity? _identity;

    private readonly ConcurrentDictionary<string, TOptions?> _lastOptions = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ITestNodeHost> _nodes = new(StringComparer.Ordinal);
    private readonly ServerPeer[] _peers;
    private readonly Func<ClusterNode, ClusterNode[], TOptions?, CancellationToken, ValueTask<ITestNodeHost>> _startNode;
    private readonly Lock _stopLock = new();
    private readonly Dictionary<string, Task> _stoppingNodes = [with(StringComparer.Ordinal)];
    private readonly ClusterNode[] _topology;
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
        _topology = topology;
        _startNode = startNode;
        _peers = peers ?? [];
        _identity = identity;
        DataDir = dataDir;
    }

    /// <summary>Gets the persistence root owned by the cluster, or <see langword="null" /> when persistence was not requested.</summary>
    public TempDirectory? DataDir { get; }

    /// <summary>Gets the peer set built from the topology, or an empty set when none was supplied.</summary>
    public IReadOnlyList<ServerPeer> Peers => _peers;

    /// <summary>Gets the number of nodes that are currently running.</summary>
    public int StartedCount => _nodes.Count;

    /// <summary>Gets the topology owned by the cluster.</summary>
    public IReadOnlyList<ClusterNode> Topology => _topology;

    /// <summary>Gets the started node with the supplied identifier.</summary>
    /// <param name="nodeId">Node identifier.</param>
    /// <returns>The started test node host.</returns>
    /// <exception cref="KeyNotFoundException">Thrown when the node was never started or was already stopped.</exception>
    public ITestNodeHost this[string nodeId] => _nodes.TryGetValue(nodeId, out var node) ? node : throw new KeyNotFoundException($"Cluster node '{nodeId}' is not running.");

    /// <summary>Stops the node and starts it again from the same topology entry.</summary>
    /// <param name="nodeId">Node identifier to restart.</param>
    /// <param name="options">
    /// Startup options for the restarted node. When <see langword="null" />, the node restarts with the options
    /// it was last started with, so persistence and other overrides survive the restart unless explicitly changed.
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The restarted test node host.</returns>
    public async ValueTask<ITestNodeHost> RestartNodeAsync(string nodeId, TOptions? options = null, CancellationToken cancellationToken = default)
    {
        var effectiveOptions = options ?? (_lastOptions.TryGetValue(nodeId, out var lastOptions) ? lastOptions : null);
        await StopNodeAsync(nodeId).ConfigureAwait(false);
        return await StartNodeAsync(nodeId, effectiveOptions, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Starts every topology entry in order, rolling the whole cluster back on any failure.</summary>
    /// <param name="factory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="releaseUnstarted">
    /// Invoked once per topology index that never started, only when a start fails, so the caller can release
    /// resources (for example a held port) reserved for that entry. Topology entries start strictly in order, so
    /// <see cref="StartedCount" /> at the point of failure is exactly the first unstarted index; every callback
    /// covers an index whose node was never handed to a caller, so it is safe even if that index's own resources
    /// were already released elsewhere (for example the failing start's own port-release-on-failure path).
    /// </param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>This cluster, with every topology entry started.</returns>
    /// <exception cref="InvalidOperationException">Thrown when some topology entries are already running.</exception>
    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Best-effort rollback: the cluster disposal failure is logged and swallowed so the original start failure always propagates.")]
    public async ValueTask<TestCluster<TOptions>> StartAllAsync(
        Func<string, TOptions?>? factory = null,
        Action<int>? releaseUnstarted = null,
        CancellationToken cancellationToken = default)
    {
        // Checked before the rollback scope: the rollback-index invariant only holds for an empty cluster,
        // and rejecting this call must not tear down nodes the caller already started individually.
        if (StartedCount != 0)
            throw new InvalidOperationException("StartAllAsync requires an empty cluster; some topology entries are already running.");

        try
        {
            for (var i = 0; i < _topology.Length; i++)
                _ = await StartNodeAsync(_topology[i].NodeId, factory?.Invoke(_topology[i].NodeId), cancellationToken).ConfigureAwait(false);

            return this;
        }
        catch
        {
            // Stop the nodes that did start, then release the held resources of nodes that never bound.
            // The start failure caught here must always propagate, even if disposal itself fails.
            var started = StartedCount;
            try
            {
                await DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception disposeException)
            {
                TestLog.Suppressed("StartAllAsync rollback: cluster disposal failed after a node start failure.", disposeException);
            }

            if (releaseUnstarted == null)
                throw;

            for (var i = started; i < _topology.Length; i++)
                releaseUnstarted(i);

            throw;
        }
    }

    /// <summary>Starts the topology entry with the supplied identifier.</summary>
    /// <param name="nodeId">Node identifier from the cluster topology.</param>
    /// <param name="options">Optional startup options for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started test node host.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="nodeId" /> is not part of the topology.</exception>
    public async ValueTask<ITestNodeHost> StartNodeAsync(string nodeId, TOptions? options = null, CancellationToken cancellationToken = default)
    {
        var host = await StartNodeAsync(FindTopologyNode(nodeId), _topology, options, cancellationToken).ConfigureAwait(false);

        // Record only after a successful start: a rejected start (already running, cancelled, faulted)
        // must not overwrite the options a later RestartNodeAsync(nodeId) falls back to.
        _lastOptions[nodeId] = options;
        return host;
    }

    /// <summary>Starts a node from an explicit topology that may differ from the cluster's own.</summary>
    /// <remarks>
    /// Only for divergence and negative tests: the node identifier may be outside <see cref="Topology" />,
    /// and this start does not update <see cref="Topology" /> or <see cref="Peers" />. A failed start is not
    /// registered, so callers can await it and assert the thrown exception. This start does not record the
    /// options used for a later restart either, so a subsequent <see cref="RestartNodeAsync" /> for the same
    /// identifier falls back to the cluster's own topology entry and last recorded options, not to the values
    /// passed here.
    /// </remarks>
    /// <param name="node">Node to start, including its identifier and listen URI.</param>
    /// <param name="topology">Topology presented to the node for this start.</param>
    /// <param name="options">Optional startup options for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The started test node host, registered under <paramref name="node" />'s identifier.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> is <see langword="null" />.</exception>
    /// <exception cref="InvalidOperationException">Thrown when a node with <paramref name="node" />'s identifier is already running.</exception>
    /// <exception cref="ObjectDisposedException">Thrown when the cluster is disposed before or during this start.</exception>
    public async ValueTask<ITestNodeHost> StartNodeAsync(ClusterNode node, ClusterNode[] topology, TOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topology);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        if (_nodes.ContainsKey(node.NodeId))
            throw new InvalidOperationException($"Cluster node '{node.NodeId}' is already running; stop it before starting it again.");

        var host = await _startNode(node, topology, options, cancellationToken).ConfigureAwait(false);
        if (!_nodes.TryAdd(node.NodeId, host))
        {
            // Another start for the same identifier won the race while this one was in flight; the
            // host was never handed to a caller, so this call is responsible for shutting it down.
            await host.ShutdownAsync().ConfigureAwait(false);
            throw new InvalidOperationException($"Cluster node '{node.NodeId}' is already running; stop it before starting it again.");
        }

        // Close the race with a concurrent DisposeAsync: if disposal's node sweep ran before this
        // registration landed, the host would otherwise survive untracked with its shared identity
        // and data directory already torn down. Shut it down regardless of whether the registration
        // is still present to remove: DisposeAsync's own sweep may already have cleared it (in which
        // case that sweep also already shut this host down), and ShutdownAsync is idempotent, so a
        // possible double shutdown is strictly safer than skipping it and leaking an untracked host.
        if (Volatile.Read(ref _disposed) != 1)
            return host;

        _ = _nodes.TryRemove(node.NodeId, out _);
        await host.ShutdownAsync().ConfigureAwait(false);
        throw new ObjectDisposedException(nameof(TestCluster<>), "The cluster was disposed while the node was starting.");
    }

    /// <summary>Stops and removes one node while leaving the rest of the cluster running.</summary>
    /// <remarks>Stopping a node that is already stopped is a no-op; the identifier must still belong to the topology.</remarks>
    /// <param name="nodeId">Node identifier to stop.</param>
    /// <returns>A task that completes after the node stopped.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="nodeId" /> is not part of the cluster topology.</exception>
    public async ValueTask StopNodeAsync(string nodeId)
    {
        Task? stopTask = null;
        lock (_stopLock)
        {
            // DisposeAsync performs a graceful shutdown for a still-running node (see ITestNodeHost), so this
            // is equivalent to ShutdownAsync while staying a call CA2000 recognizes as disposal. The stop is
            // registered so a concurrent cluster DisposeAsync waits for it before releasing shared resources.
            if (_nodes.TryRemove(nodeId, out var node))
                _stoppingNodes[nodeId] = stopTask = node.DisposeAsync().AsTask();
        }

        if (stopTask != null)
        {
            try
            {
                await stopTask.ConfigureAwait(false);
            }
            finally
            {
                lock (_stopLock)
                    _ = _stoppingNodes.Remove(nodeId);
            }

            return;
        }

        // Not currently running is fine (a repeat stop is a no-op); an id outside the topology is
        // almost always a typo or a stale reference, so fail loudly instead of silently doing nothing.
        if (!IsTopologyNode(nodeId))
            throw new ArgumentException($"Node '{nodeId}' is not part of the cluster topology.", nameof(nodeId));
    }

    /// <inheritdoc />
    /// <exception cref="AggregateException">
    /// Thrown when one or more nodes fail to shut down. Every node is still given a shutdown attempt, and the
    /// shared identity and data directory are still disposed, regardless of individual node failures.
    /// </exception>
    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Every node must get a shutdown attempt regardless of another node's failure; failures are aggregated and rethrown.")]
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        Task[] pendingStops;
        lock (_stopLock)
            pendingStops = [.. _stoppingNodes.Values];

        foreach (var pending in pendingStops)
        {
            try
            {
                await pending.ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // Observed by the original StopNodeAsync caller; this wait only needs the in-flight
                // stop to finish — successfully or not — before the shared identity/data directory
                // below are released.
                TestLog.Suppressed("DisposeAsync: an in-flight StopNodeAsync failed; its caller observes the failure.", ex);
            }
        }

        List<Exception>? failures = null;
        foreach (var node in _nodes.Values)
        {
            try
            {
                await node.ShutdownAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                (failures ??= []).Add(ex);
            }
        }

        _nodes.Clear();
        _identity?.Dispose();
        DataDir?.Dispose();

        if (failures != null)
            throw new AggregateException("One or more cluster nodes failed to shut down.", failures);
    }

    /// <summary>Creates a single-node cluster with in-process nodes.</summary>
    /// <param name="node">The only topology entry.</param>
    /// <param name="peers">Optional peer set built from the topology, exposed via <see cref="Peers" />.</param>
    /// <param name="identity">Optional shared mTLS identity disposed with the cluster.</param>
    /// <param name="dir">
    /// Optional persistence root name. When supplied, the cluster creates and owns a <see cref="TempDirectory" />
    /// under this name, disposed with the cluster and exposed through <see cref="DataDir" />.
    /// </param>
    /// <returns>A cluster that starts in-process nodes.</returns>
    internal static TestCluster<TOptions> Create(ClusterNode node, ServerPeer[]? peers = null, ClusterIdentity? identity = null, string? dir = null)
    {
        var sharedIdentity = ClusterIdentity.ResolveForTopology([node], identity);

        // An empty hint suppresses the implicit [CallerMemberName] leaf, so disposal deletes exactly the created directory.
        var dataDir = dir == null ? null : new TempDirectory(dir, string.Empty);
        return new TestCluster<TOptions>(
            node,
            (self, topology, options, cancellationToken) => StartNodeCoreAsync(self, topology, options, sharedIdentity, cancellationToken),
            peers,
            sharedIdentity,
            dataDir);
    }

    /// <summary>Creates a cluster with in-process nodes.</summary>
    /// <param name="topology">Cluster members owned by the cluster.</param>
    /// <param name="peers">Optional peer set built from the topology, exposed via <see cref="Peers" />.</param>
    /// <param name="identity">Optional shared mTLS identity disposed with the cluster.</param>
    /// <param name="dir">
    /// Optional persistence root name. When supplied, the cluster creates and owns a <see cref="TempDirectory" />
    /// under this name, disposed with the cluster and exposed through <see cref="DataDir" />.
    /// </param>
    /// <returns>A cluster that starts in-process nodes.</returns>
    internal static TestCluster<TOptions> Create(ClusterNode[] topology, ServerPeer[]? peers = null, ClusterIdentity? identity = null, string? dir = null)
    {
        var sharedIdentity = ClusterIdentity.ResolveForTopology(topology, identity);
        return new TestCluster<TOptions>(
            topology,
            (self, entries, options, cancellationToken) => StartNodeCoreAsync(self, entries, options, sharedIdentity, cancellationToken),
            peers,
            sharedIdentity,
            dir == null ? null : new TempDirectory(dir, string.Empty));
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

    private static NodeHostStartOptions CreateOptions(
        ClusterStartOptions? startOptions,
        PersistenceOptions? persistenceOptions,
        Func<string, HttpMessageHandler>? peerHandlerFactory,
        MtlsOptions? mtlsOptions,
        MtlsCertificate? mtlsMaterial)
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
            PersistenceOptions = persistenceOptions,
            PeerHandlerFactory = peerHandlerFactory,
            SecurityOptions = startOptions?.Security?.ToServerOptions(),
            MtlsOptions = mtlsOptions,
            Certificate = mtlsMaterial,
            TimeProvider = startOptions?.TimeProvider,
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

    private ClusterNode FindTopologyNode(string nodeId)
    {
        for (var i = 0; i < _topology.Length; i++)
        {
            if (string.Equals(_topology[i].NodeId, nodeId, StringComparison.Ordinal))
                return _topology[i];
        }

        throw new ArgumentException($"Node '{nodeId}' is not part of the cluster topology.", nameof(nodeId));
    }

    private bool IsTopologyNode(string nodeId)
    {
        for (var i = 0; i < _topology.Length; i++)
        {
            if (string.Equals(_topology[i].NodeId, nodeId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
