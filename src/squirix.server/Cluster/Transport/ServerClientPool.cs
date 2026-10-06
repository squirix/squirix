using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Holds gRPC clients per peer and an execution policy per peer.</summary>
/// <remarks>
/// Disposal runs in a fixed order under one shutdown budget: new leases are refused and in-flight calls cancelled and awaited, then the channels
/// and their handlers are disposed, then every tracked connection (an open TLS handshake included) is awaited, and only then is the pool's hold on
/// the mTLS material released. A connection that outlives the budget keeps the material loaded until the connection ends, which is logged, rather than freeing the node
/// certificate under a handshake that still reads it.
/// </remarks>
[Mutable]
internal sealed class ServerClientPool : IServerClientPool
{
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(10);

    /// <summary>The longest finite timeout <see cref="Task.WaitAsync(TimeSpan, TimeProvider)" /> accepts.</summary>
    private static readonly TimeSpan MaxShutdownBudget = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new(StringComparer.Ordinal);

    /// <summary>In-flight calls that lease a channel outside the peer policies; drained before the channels are disposed.</summary>
    private readonly QuiescenceGate _calls = new();

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.Ordinal);

    /// <summary>Cancels every leased call once disposal starts.</summary>
    private readonly CancellationTokenSource _closing = new();

    /// <summary>Open connections of the owned handlers, each from its connect attempt until its stream is disposed; drained before the material is released.</summary>
    private readonly TrackedConnections _connections = new();

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
        if (args.ShutdownBudget is { } budget && budget != Timeout.InfiniteTimeSpan && (budget < TimeSpan.Zero || budget > MaxShutdownBudget))
            throw new ArgumentOutOfRangeException(nameof(args), budget, "The shutdown budget must be between zero and the longest timeout a task wait accepts, or infinite.");

        _shutdownBudget = args.ShutdownBudget ?? DefaultShutdownBudget;
        _timeProvider = args.TimeProvider ?? TimeProvider.System;
        _materialHold = args.Certificate is { Enabled: true } certificate ? certificate.Retain() : null;
        var nodeIds = new string[peers.Count];
        try
        {
            for (var i = 0; i < peers.Count; i++)
            {
                var peer = peers[i];
                RegisterPeer(peer, args);
                nodeIds[i] = peer.NodeId;
            }
        }
        catch
        {
            foreach (var channel in _channels.Values)
                _ = Isolated.Run(channel, static created => created.Dispose());

            _materialHold?.Dispose();
            _closing.Dispose();
            throw;
        }

        Array.Sort(nodeIds, StringComparer.Ordinal);
        _nodeIds = nodeIds;
        NodeIds = _nodeIds;
    }

    internal IReadOnlyCollection<string> NodeIds { get; }

    /// <summary>Gets the task that releases the material hold late after a connection drain timeout; completed when disposal released the hold in time.</summary>
    internal Task LateMaterialRelease { get; private set; } = Task.CompletedTask;

    /// <summary>Gets the number of connections of the owned handlers that are still open.</summary>
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
        var channel = _channels[nodeId];
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

    private static GrpcChannelOptions CreateChannelOptions(
        string nodeId,
        bool interNodeMtlsEnabled,
        MtlsCertificate? certificate,
        Func<string, HttpMessageHandler>? peerHandlerFactory,
        Func<MtlsCertificate?, string, TrackedConnections, HttpMessageHandler> ownedHandlerFactory,
        TrackedConnections connections)
    {
        HttpMessageHandler? ownedHandler = null;
        try
        {
            HttpMessageHandler peerHandler;
            if (interNodeMtlsEnabled)
            {
                if (certificate is not { Enabled: true })
                    throw new InvalidOperationException("Cluster mTLS material must be loaded for internode transport.");

                var factoryHandler = peerHandlerFactory?.Invoke(nodeId);
                if (factoryHandler != null)
                {
                    peerHandler = factoryHandler;
                }
                else
                {
                    ownedHandler = ownedHandlerFactory.Invoke(certificate, nodeId, connections);
                    peerHandler = ownedHandler;
                }
            }
            else
            {
                ownedHandler = ownedHandlerFactory.Invoke(null, nodeId, connections);
                peerHandler = ownedHandler;
            }

            var options = new GrpcChannelOptions
            {
                HttpHandler = peerHandler,

                // The channel disposes the handler the pool created; a peerHandlerFactory handler stays owned by the factory's caller. The
                // certificates an owned handler presents stay loaded through the pool's material hold until its connections are gone.
                DisposeHttpClient = ownedHandler != null,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            };
            ownedHandler = null;
            return options;
        }
        finally
        {
            ownedHandler?.Dispose();
        }
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
            var failure = Isolated.Run(_channels[nodeId], static channel => channel.Dispose());
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
            await _connections.WaitAsync(CancellationToken.None).AsTask().WaitAsync(Remaining(started), _timeProvider, CancellationToken.None).ConfigureAwait(false);
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
            LateMaterialRelease = ReleaseMaterialWhenDrainedAsync(_materialHold);
        }
    }

    /// <summary>Releases the pool's hold once the connections that outlived the budget are gone; the task never faults, a failure is logged and the hold is kept.</summary>
    /// <param name="hold">The pool's hold on the material.</param>
    /// <returns>A task that completes once the hold was released or the wait failed.</returns>
    private async Task ReleaseMaterialWhenDrainedAsync(MtlsCertificate.Hold hold)
    {
        var failure = await _connections.WaitAsync(CancellationToken.None).AsTask().CaptureFailureAsync().ConfigureAwait(false);
        if (failure != null)
        {
            ServerLog.ClientPoolLateMaterialReleaseFailed(_logger, failure);
            return;
        }

        hold.Dispose();
        ServerLog.ClientPoolMaterialReleasedLate(_logger);
    }

    private void RegisterPeer(ServerPeer peer, ServerClientPoolArgs args)
    {
        var mtlsOptions = args.MtlsOptions ?? new MtlsOptions();
        var address = ClusterPeerChannelAddress.Resolve(peer, mtlsOptions, args.InterNodeMtlsEnabled);
        var ownedHandlerFactory = args.OwnedHandlerFactory ?? ServerGrpcEndpoints.CreateOwnedHandler;
        var options = CreateChannelOptions(peer.NodeId, args.InterNodeMtlsEnabled, args.Certificate, args.PeerHandlerFactory, ownedHandlerFactory, _connections);
        var channel = GrpcChannel.ForAddress(address, options);
        var invoker = channel.CreateCallInvoker();
        if (args.InternalOwnerInterceptor != null)
            invoker = invoker.Intercept(args.InternalOwnerInterceptor);
        if (args.Interceptor != null)
            invoker = invoker.Intercept(args.Interceptor);

        _channels[peer.NodeId] = channel;
        _cacheClients[peer.NodeId] = new SquirixCacheService.SquirixCacheServiceClient(invoker);
        _policies[peer.NodeId] = args.PolicyFactory.Invoke(peer.NodeId);
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

    /// <summary>Validates and configures gRPC transport endpoints for server-to-server transport.</summary>
    private static class ServerGrpcEndpoints
    {
        /// <summary>Bounds a connect attempt, TLS handshake included, so a peer that accepts but never answers cannot hold a connection open past shutdown.</summary>
        private static readonly TimeSpan InterNodeConnectTimeout = TimeSpan.FromSeconds(5);

        private static readonly List<SslApplicationProtocol> Http2PreferredProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11];

        /// <summary>Creates the handler a pool owns for one peer: mTLS when <paramref name="certificate" /> is supplied, plain HTTPS otherwise.</summary>
        /// <param name="certificate">Loaded cluster mTLS material, or <see langword="null" /> for plain HTTPS.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <param name="connections">The pool's tracked connections of the handler.</param>
        /// <returns>A handler owned by the caller.</returns>
        internal static SocketsHttpHandler CreateOwnedHandler(MtlsCertificate? certificate, string expectedPeerNodeId, TrackedConnections connections) =>
            certificate == null ? CreateChannelHandler(connections) : CreateMtlsHandler(certificate, expectedPeerNodeId, connections);

        /// <summary>Creates the default HTTP handler for HTTPS gRPC channels.</summary>
        /// <param name="connections">The pool's tracked connections.</param>
        /// <returns>A handler suitable for secure gRPC transport that opens more HTTP/2 connections once the stream limit is reached.</returns>
        private static SocketsHttpHandler CreateChannelHandler(TrackedConnections connections) => new()
        {
            EnableMultipleHttp2Connections = true,
            ConnectTimeout = InterNodeConnectTimeout,
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

        /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
        /// <param name="clientCertificate">Client certificate presented to the peer.</param>
        /// <param name="trustAnchor">Configured cluster trust root.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <param name="connections">The pool's tracked connections of the handler.</param>
        /// <returns>A handler configured for internode mutual TLS.</returns>
        private static SocketsHttpHandler CreateMtlsHandler(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId, TrackedConnections connections)
        {
            ArgumentNullException.ThrowIfNull(clientCertificate);
            ArgumentNullException.ThrowIfNull(trustAnchor);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

            return new SocketsHttpHandler
            {
                UseProxy = false,
                EnableMultipleHttp2Connections = true,
                ConnectTimeout = InterNodeConnectTimeout,
                ConnectCallback = (context, cancellationToken) => TrackedConnectionStream.ConnectAsync(connections, context, cancellationToken),
                SslOptions = new SslClientAuthenticationOptions
                {
                    ClientCertificates = [clientCertificate],
                    ApplicationProtocols = Http2PreferredProtocols,
                    RemoteCertificateValidationCallback = (_, certificate, _, _) => ValidatePeerServerCertificate(certificate, trustAnchor, expectedPeerNodeId),
                },
            };
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
}
