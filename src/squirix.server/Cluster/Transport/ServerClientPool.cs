using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Holds gRPC clients per peer and an execution policy per peer.</summary>
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

    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new(StringComparer.Ordinal);

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
    private readonly ConcurrentDictionary<string, IServerCallPolicy> _policies = new(StringComparer.Ordinal);

    private readonly TimeSpan _shutdownBudget;
    private readonly TimeProvider _timeProvider;
    private int _disposed;

    internal ServerClientPool(IReadOnlyList<ServerPeer> peers, ServerClientPoolArgs args, ServerClientPoolMetrics metrics, ILogger<ServerClientPool> logger)
    {
        ArgumentNullException.ThrowIfNull(args);
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
        _metrics = metrics;
        _shutdownBudget = ResolveShutdownBudget(args);
        _forwardConnectTimeout = args.ForwardConnectTimeout ?? TopologyOptions.DefaultForwardConnectTimeout;
        _timeProvider = args.TimeProvider ?? TimeProvider.System;
        _materialHold = RetainMaterial(args);
        _nodeIds = RegisterPeers(peers, args);
        NodeIds = _nodeIds;
    }

    internal IReadOnlyCollection<string> NodeIds { get; }

    /// <summary>Gets the task that releases the material hold late after a connection drain timeout; completed when disposal released the hold in time.</summary>
    internal Task LateMaterialRelease => _connections.LateRelease;

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

    public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => _cacheClients[nodeId];

    public ServerChannelLease LeaseChannel(string nodeId, CancellationToken cancellationToken)
    {
        var channel = _channels[nodeId].Lease;
        if (!_calls.TryEnter())
            throw new ObjectDisposedException(nameof(ServerClientPool), "The server client pool is disposing and leases no channel.");

        try
        {
            return new ServerChannelLease(channel, CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _closing.Token), _calls);
        }
        catch
        {
            _calls.Exit();
            throw;
        }
    }

    public IServerCallPolicy PolicyFor(string nodeId) => _policies[nodeId];

    /// <summary>Makes the connections of a handler the pool does not own count in the pool's gate, so disposal drains and aborts them before the material is released.</summary>
    /// <param name="handler">The handler a factory supplied; one that is not, or does not wrap, a <see cref="SocketsHttpHandler" /> stays untracked.</param>
    /// <param name="connections">The pool's tracked connections.</param>
    /// <remarks>An existing connect callback keeps dialing; the tracked callback wraps the stream it returns.</remarks>
    /// <exception cref="InvalidOperationException">The handler's connect callback already tracks the connections of a pool, so a factory returned one handler twice.</exception>
    internal static void TrackFactoryConnections(HttpMessageHandler handler, TrackedConnections connections)
    {
        var current = handler;
        while (current is DelegatingHandler { InnerHandler: { } next })
            current = next;

        if (current is not SocketsHttpHandler socketsHandler)
            return;

        var inner = socketsHandler.ConnectCallback;
        if (inner?.Target is TrackingConnectCallback)
            throw new InvalidOperationException("The peer handler factory returned a handler that already belongs to a pool; it must create a fresh handler per call.");

        socketsHandler.ConnectCallback = new TrackingConnectCallback(connections, inner).ConnectAsync;
    }

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
            : throw new ArgumentOutOfRangeException(
                nameof(args),
                budget,
                "The shutdown budget must be between zero and the longest timeout a task wait accepts, or infinite.");
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
        var channels = PeerChannels.Create(peer.NodeId, address, args, _connections, _forwardConnectTimeout);
        var invoker = channels.Forward.CreateCallInvoker();
        if (args.InternalOwnerInterceptor != null)
            invoker = invoker.Intercept(args.InternalOwnerInterceptor);
        if (args.Interceptor != null)
            invoker = invoker.Intercept(args.Interceptor);

        _channels[peer.NodeId] = channels;
        _cacheClients[peer.NodeId] = new SquirixCacheService.SquirixCacheServiceClient(invoker);
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
        foreach (var channels in _channels)
            _ = channels.Value.Close();

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

    /// <summary>Validates and configures gRPC transport endpoints for server-to-server transport.</summary>
    /// <remarks>The pool sets the connect timeout of every handler created here, so a peer that accepts but never answers cannot hold a connection open.</remarks>
    internal static class ServerGrpcEndpoints
    {
        private static readonly List<SslApplicationProtocol> Http2PreferredProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11];

        /// <summary>Creates the handler a pool owns for one peer: mTLS when <paramref name="certificate" /> is supplied, plain HTTPS otherwise.</summary>
        /// <param name="certificate">Loaded cluster mTLS material, or <see langword="null" /> for plain HTTPS.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <param name="connections">The pool's tracked connections of the handler.</param>
        /// <returns>A handler owned by the caller.</returns>
        internal static SocketsHttpHandler CreateOwnedHandler(MtlsCertificate? certificate, string expectedPeerNodeId, TrackedConnections connections) =>
            certificate == null ? CreateChannelHandler(connections) : CreateMtlsHandler(certificate, expectedPeerNodeId, connections);

        /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
        /// <param name="clientCertificate">Client certificate presented to the peer.</param>
        /// <param name="trustAnchor">Configured cluster trust root.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <param name="connections">The pool's tracked connections of the handler.</param>
        /// <returns>A handler configured for internode mutual TLS.</returns>
        internal static SocketsHttpHandler CreateMtlsHandler(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId, TrackedConnections connections)
        {
            ArgumentNullException.ThrowIfNull(clientCertificate);
            ArgumentNullException.ThrowIfNull(trustAnchor);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

            // On Unix, SslStream builds a client certificate chain on every handshake before the ClientHello, inside the connect timeout.
            // A context prebuilt offline once per peer removes that work and never fetches anything.
            // Windows starts mutual auth anonymously and needs no context; supplying one up front fails for ephemeral keys.
            var clientCertificateContext = OperatingSystem.IsWindows() ? null : SslStreamCertificateContext.Create(clientCertificate, null, true);
            return new SocketsHttpHandler
            {
                UseProxy = false,
                EnableMultipleHttp2Connections = true,
                ConnectCallback = (context, cancellationToken) => TrackedConnectionStream.ConnectAsync(connections, context, cancellationToken),
                SslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [clientCertificate],
                    ClientCertificateContext = clientCertificateContext,
                    ApplicationProtocols = Http2PreferredProtocols,
                    RemoteCertificateValidationCallback = (_, certificate, _, _) => ValidatePeerServerCertificate(certificate, trustAnchor, expectedPeerNodeId),
                },
            };
        }

        /// <summary>Creates the default HTTP handler for HTTPS gRPC channels.</summary>
        /// <param name="connections">The pool's tracked connections.</param>
        /// <returns>A handler suitable for secure gRPC transport that opens more HTTP/2 connections once the stream limit is reached.</returns>
        private static SocketsHttpHandler CreateChannelHandler(TrackedConnections connections) => new()
        {
            EnableMultipleHttp2Connections = true,
            ConnectCallback = (context, cancellationToken) => TrackedConnectionStream.ConnectAsync(connections, context, cancellationToken),
        };

        /// <summary>Creates an outbound cluster mTLS HTTP handler that presents the local node certificate.</summary>
        /// <param name="certificate">Loaded cluster mTLS certificate.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <param name="connections">The pool's tracked connections of the handler.</param>
        /// <returns>A handler configured for internode mutual TLS.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="certificate" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when cluster mTLS certificate is not loaded.</exception>
        private static SocketsHttpHandler CreateMtlsHandler(MtlsCertificate certificate, string expectedPeerNodeId, TrackedConnections connections)
        {
            ArgumentNullException.ThrowIfNull(certificate);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);
            var missingMaterial = !certificate.Enabled || certificate.NodeCertificate == null || certificate.TrustAnchor == null;
            const string message = "Cluster mTLS certificate must be loaded before creating the outbound handler.";
            return missingMaterial ? throw new InvalidOperationException(message) : CreateMtlsHandler(certificate.NodeCertificate!, certificate.TrustAnchor!, expectedPeerNodeId, connections);
        }

        /// <summary>Validates a peer server certificate against the configured cluster trust root.</summary>
        /// <param name="serverCertificate">The presented peer server certificate.</param>
        /// <param name="trustAnchor">Configured cluster trust root.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <returns><see langword="true" /> when the certificate is trusted for internode traffic.</returns>
        private static bool ValidatePeerServerCertificate(X509Certificate? serverCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
        {
            if (serverCertificate == null)
                return false;

            using var certificate = new X509Certificate2(serverCertificate);
            return MtlsClientCertificateValidator.ValidateForExpectedNodeId(certificate, trustAnchor, expectedPeerNodeId);
        }
    }

    /// <summary>Resolves gRPC channel addresses for internode cluster transport.</summary>
    private static class ClusterPeerChannelAddress
    {
        /// <summary>Resolves the gRPC endpoint used for internode cluster calls.</summary>
        /// <param name="peer">Configured cluster peer.</param>
        /// <param name="options">Cluster mTLS options for the local node.</param>
        /// <param name="mtlsEnabled">Whether internode mTLS transport is active.</param>
        /// <returns>The HTTPS gRPC address for pooled cluster clients.</returns>
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

    /// <summary>The connect callback the pool installs on a factory-supplied handler; its type marks a handler that already belongs to a pool.</summary>
    [Immutable]
    private sealed class TrackingConnectCallback
    {
        private readonly TrackedConnections _connections;
        private readonly Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>? _inner;

        internal TrackingConnectCallback(TrackedConnections connections, Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>? inner)
        {
            _connections = connections;
            _inner = inner;
        }

        internal ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken) =>
            _inner == null
                ? TrackedConnectionStream.ConnectAsync(_connections, context, cancellationToken)
                : TrackedWrappedStream.ConnectAsync(_connections, context, _inner, cancellationToken);
    }
}
