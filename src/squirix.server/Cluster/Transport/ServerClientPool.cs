using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Holds native clients per peer and an execution policy per peer.</summary>
/// <remarks>
/// <para>
/// Every peer has two channels (<see cref="PeerChannels" />): forwarded client calls go through one whose dial is bounded by the forward
/// connect timeout, every leased call through one without that bound.
/// </para>
/// <para>
/// Disposal runs in a fixed order under one shutdown budget: new leases are refused and in-flight calls cancelled and awaited, then the channels
/// and their handlers are disposed, then every tracked connection (an open TLS handshake included) is awaited, and only then is the pool's hold on
/// the mTLS material released. A connection that outlives the budget keeps the material loaded until the connection ends, which is logged, rather than freeing the node
/// certificate under a handshake that still reads it.
/// </para>
/// </remarks>
[Mutable]
internal sealed class ServerClientPool : IServerClientPool
{
    /// <summary>The least time aborted connections get to dispose, even when the call drain used up the whole shutdown budget.</summary>
    private static readonly TimeSpan ConnectionAbortGrace = TimeSpan.FromMilliseconds(250);

    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(10);

    /// <summary>The longest finite timeout <see cref="Task.WaitAsync(TimeSpan, TimeProvider)" /> accepts.</summary>
    private static readonly TimeSpan MaxShutdownBudget = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly ConcurrentDictionary<string, IPeerClient> _cacheClients = new(StringComparer.Ordinal);

    /// <summary>In-flight calls that lease a channel outside the peer policies; drained before the channels are disposed.</summary>
    private readonly QuiescenceGate _calls = new();

    private readonly ConcurrentDictionary<string, PeerChannels> _channels = new(StringComparer.Ordinal);

    /// <summary>Cancels every leased call once disposal starts.</summary>
    private readonly CancellationTokenSource _closing = new();

    /// <summary>Open connections of the pool's handlers, owned or factory-supplied, each from its connect attempt until its stream is disposed; drained before the material is released.</summary>
    private readonly TrackedConnections _connections = new();

    /// <summary>Bounds the dial of a forward channel, before any TLS handshake.</summary>
    private readonly TimeSpan _forwardConnectTimeout;

    private readonly ILogger<ServerClientPool> _logger;

    /// <summary>The pool's hold on the mTLS material its owned handlers present; <see langword="null" /> without enabled material.</summary>
    private readonly MtlsCertificate.Hold? _materialHold;

    private readonly ServerClientPoolMetrics _metrics;
    private readonly string[] _nodeIds;

    /// <summary>Checks the certificates peers present to the owned handlers and logs the refusals; <see langword="null" /> without enabled material.</summary>
    private readonly PeerHandshakeVerifier? _peerVerifier;

    private readonly ConcurrentDictionary<string, IServerCallPolicy> _policies = new(StringComparer.Ordinal);

    private readonly TimeSpan _shutdownBudget;
    private readonly TimeProvider _timeProvider;
    private int _disposed;

    internal ServerClientPool(IReadOnlyList<ServerPeer> peers, ServerClientPoolArgs args, ServerClientPoolMetrics metrics, ILogger<ServerClientPool> logger, ILogger peerHandshakeLogger)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(peerHandshakeLogger);
        _logger = logger;
        _metrics = metrics;
        _shutdownBudget = ResolveShutdownBudget(args);
        _forwardConnectTimeout = args.ForwardConnectTimeout ?? TopologyOptions.DefaultForwardConnectTimeout;
        _timeProvider = args.TimeProvider;
        _materialHold = RetainMaterial(args);
        _peerVerifier = args.Certificate is { Enabled: true, PeerChains: { } peerChains } ? new PeerHandshakeVerifier(peerChains, TimeProvider.System, peerHandshakeLogger) : null;
        _nodeIds = RegisterPeers(peers, args);
        NodeIds = _nodeIds;
    }

    internal IReadOnlyCollection<string> NodeIds { get; }

    /// <summary>Gets the number of connections of the pool's handlers, owned or factory-supplied, that are still open.</summary>
    internal int OpenConnections => _connections.Pending;

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        var started = _timeProvider.GetTimestamp();
        var callsDrained = await DrainCallsAsync(started).ConfigureAwait(false);
        DisposeChannels();
        var connectionsDrained = await DrainConnectionsAsync(started).ConfigureAwait(false);
        ReleaseMaterial(connectionsDrained);

        // A lease that outlived the budget still links its token to this source; it is left alone rather than disposed under the call.
        if (callsDrained)
            _closing.Dispose();
    }

    public IPeerClient ForNode(string nodeId) => _cacheClients[nodeId];

    public ServerChannelLease LeaseChannel(string nodeId, CancellationToken cancellationToken)
    {
        var channels = _channels[nodeId];
        if (!_calls.TryEnter())
            throw new ObjectDisposedException(nameof(ServerClientPool), "The server client pool is disposing and leases no channel.");

        try
        {
            if (!cancellationToken.CanBeCanceled)
                return new ServerChannelLease(channels.Replication, null, _calls, _closing.Token);

            var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closing.Token);
            return new ServerChannelLease(channels.Replication, linked, _calls, linked.Token);
        }
        catch
        {
            _calls.Exit();
            throw;
        }
    }

    public IServerCallPolicy PolicyFor(string nodeId) => _policies[nodeId];

    /// <summary>Takes a hold on the mTLS material when it is enabled.</summary>
    /// <param name="args">The pool arguments.</param>
    /// <returns>The hold, or <see langword="null" /> without enabled material.</returns>
    private static MtlsCertificate.Hold? RetainMaterial(ServerClientPoolArgs args) => args.Certificate is { Enabled: true } certificate ? certificate.Retain() : null;

    /// <summary>Validates the configured shutdown budget or falls back to the default.</summary>
    /// <param name="args">The pool arguments.</param>
    /// <returns>The shutdown budget.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The configured budget is negative and not infinite, or above the longest accepted timeout.</exception>
    private static TimeSpan ResolveShutdownBudget(ServerClientPoolArgs args)
    {
        if (args.ShutdownBudget is not { } budget)
            return DefaultShutdownBudget;

        var valid = budget == Timeout.InfiniteTimeSpan || (budget >= TimeSpan.Zero && budget <= MaxShutdownBudget);
        return valid
            ? budget
            : throw new ArgumentOutOfRangeException(nameof(args), budget, "The shutdown budget must be between zero and the longest timeout a task wait accepts, or infinite.");
    }

    private void BeginDrain()
    {
        for (var i = 0; i < _nodeIds.Length; i++)
            _policies[_nodeIds[i]].BeginDrain();
    }

    private string BusyPeers(Task[] drains)
    {
        var busy = new List<string>(_nodeIds.Length);
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            if (!drains[i].IsCompleted)
                busy.Add(_nodeIds[i]);
        }

        return string.Join(", ", busy);
    }

    private async Task CancelLeasesAsync()
    {
        // A callback registered on a lease token may throw; the remaining callbacks have run and disposal must still release the channels and the material.
        var failure = await _closing.CancelAsync().CaptureFailureAsync().ConfigureAwait(false);
        if (failure != null)
            ServerLog.ClientPoolLeaseCancelFailed(_logger, failure);
    }

    private void DisposeChannels()
    {
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var nodeId = _nodeIds[i];
            var failure = _channels[nodeId].Close();
            if (failure == null)
                _metrics.AddDisposal();
            else
                ServerLog.ClientPoolChannelDisposeFailed(_logger, failure, nodeId);
        }
    }

    /// <summary>Refuses new leases, cancels the leased calls and waits for them and the peer policies to drain.</summary>
    /// <param name="started">The timestamp disposal started at.</param>
    /// <returns><see langword="true" /> when every call ended within the budget.</returns>
    private async Task<bool> DrainCallsAsync(long started)
    {
        // The peers drain in parallel under one budget, so a stuck peer cannot hold up host shutdown; channels are disposed either way.
        BeginDrain();
        _calls.Close();
        await CancelLeasesAsync().ConfigureAwait(false);
        var drains = new Task[_nodeIds.Length + 1];
        for (var i = 0; i < _nodeIds.Length; i++)
            drains[i] = DisposePolicyAsync(_nodeIds[i]);

        drains[_nodeIds.Length] = _calls.WaitAsync(CancellationToken.None).AsTask();
        try
        {
            await Task.WhenAll(drains).WaitAsync(Remaining(started), _timeProvider, CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            ServerLog.ClientPoolDrainTimedOut(_logger, _shutdownBudget, _calls.Pending, BusyPeers(drains));
            return false;
        }
    }

    /// <summary>Refuses new connections and waits for the open ones, handshakes included, to close.</summary>
    /// <param name="started">The timestamp disposal started at.</param>
    /// <returns><see langword="true" /> when every connection closed within the budget.</returns>
    private async Task<bool> DrainConnectionsAsync(long started)
    {
        _connections.Close();

        // The channels are gone, so a connection still open is a handshake that no call waits for; closing its socket ends it at once.
        _connections.AbortAll();
        try
        {
            // Closing the sockets is not instant; an exhausted budget still leaves the aborted connections a short grace to dispose.
            var wait = Remaining(started);
            if (wait != Timeout.InfiniteTimeSpan && wait < ConnectionAbortGrace)
                wait = ConnectionAbortGrace;

            await _connections.WaitAsync(CancellationToken.None).AsTask().WaitAsync(wait, _timeProvider, CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    private async Task DisposePolicyAsync(string nodeId)
    {
        var failure = await _policies[nodeId].CaptureFailureAsync().ConfigureAwait(false);
        if (failure != null)
            ServerLog.ClientPoolPolicyDisposeFailed(_logger, failure, nodeId);
    }

    /// <summary>Releases the pool's hold on the mTLS material, or keeps it when a connection may still read the certificates.</summary>
    /// <param name="connectionsDrained">Whether every tracked connection closed.</param>
    private void ReleaseMaterial(bool connectionsDrained)
    {
        if (_materialHold == null)
            return;

        if (connectionsDrained)
        {
            _materialHold.Dispose();
        }
        else
        {
            ServerLog.ClientPoolMaterialLeaked(_logger, _connections.Pending, _shutdownBudget);
            _connections.ReleaseWhenDrained(_materialHold, _logger);
        }
    }

    private void RegisterPeer(ServerPeer peer, ServerClientPoolArgs args)
    {
        var mtlsOptions = args.MtlsOptions ?? new MtlsOptions();
        var address = ClusterPeerChannelAddress.Resolve(peer, mtlsOptions, args.InterNodeMtlsEnabled);
        var channels = args.ChannelsOverride == null
            ? PeerChannels.Create(peer.NodeId, address, args, _peerVerifier, _connections, _forwardConnectTimeout)
            : args.ChannelsOverride(peer.NodeId, address, _peerVerifier, _connections, _forwardConnectTimeout);
        _channels[peer.NodeId] = channels;
        _cacheClients[peer.NodeId] = channels.Forward;
        _policies[peer.NodeId] = args.PolicyFactory.Invoke(peer.NodeId);
    }

    /// <summary>Registers every peer; when one fails, the channels created so far, the material hold and the closing source are released before the failure propagates.</summary>
    /// <param name="peers">The peers to register.</param>
    /// <param name="args">The pool arguments.</param>
    /// <returns>The node ids in ordinal order.</returns>
    private string[] RegisterPeers(IReadOnlyList<ServerPeer> peers, ServerClientPoolArgs args)
    {
        var nodeIds = new string[peers.Count];
        try
        {
            for (var i = 0; i < peers.Count; i++)
            {
                RegisterPeer(peers[i], args);
                nodeIds[i] = peers[i].NodeId;
            }
        }
        catch
        {
            ReleaseOnFailure();
            throw;
        }

        Array.Sort(nodeIds, StringComparer.Ordinal);
        return nodeIds;
    }

    /// <summary>Disposes the created channels, the material hold and the closing source after a failed construction.</summary>
    private void ReleaseOnFailure()
    {
        foreach (var (_, channels) in _channels)
            _ = channels.Close();

        _materialHold?.Dispose();
        _closing.Dispose();
    }

    /// <summary>The part of the shutdown budget left since <paramref name="started" />; never negative.</summary>
    /// <param name="started">The timestamp disposal started at.</param>
    /// <returns>The remaining budget.</returns>
    private TimeSpan Remaining(long started)
    {
        if (_shutdownBudget == Timeout.InfiniteTimeSpan)
            return _shutdownBudget;

        var remaining = _shutdownBudget - _timeProvider.GetElapsedTime(started);
        return remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
    }

    /// <summary>Resolves TLS channel addresses for internode cluster transport.</summary>
    private static class ClusterPeerChannelAddress
    {
        /// <summary>Resolves the peer TLS endpoint used for internode cluster calls.</summary>
        /// <param name="peer">Configured cluster peer.</param>
        /// <param name="options">Cluster mTLS options for the local node.</param>
        /// <param name="mtlsEnabled">Whether internode mTLS transport is active.</param>
        /// <returns>The peer TLS address for pooled cluster clients.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="peer" /> or <paramref name="options" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when internode mTLS is enabled but the internal listen port or peer URI is invalid.</exception>
        internal static Uri Resolve(ServerPeer peer, MtlsOptions options, bool mtlsEnabled)
        {
            ArgumentNullException.ThrowIfNull(peer);
            ArgumentNullException.ThrowIfNull(options);

            if (!mtlsEnabled)
                return peer.Uri;

            if (peer.InterNodeUri is { } uri)
                return uri;

            if (options.InternalListenPort <= 0)
                throw new InvalidOperationException("Cluster mTLS internal listen port must be configured for internode transport.");

            const string message = "Cluster peer URI is invalid.";
            var primaryUri = peer.Uri;
            return peer.Uri.IsAbsoluteUri ? new UriBuilder(primaryUri.Scheme, primaryUri.Host, options.InternalListenPort).Uri : throw new InvalidOperationException(message);
        }
    }
}
