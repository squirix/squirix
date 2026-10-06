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
[Mutable]
internal sealed class ServerClientPool : IServerClientPool
{
    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(10);

    /// <summary>The longest finite timeout <see cref="Task.WaitAsync(TimeSpan, TimeProvider)" /> accepts.</summary>
    private static readonly TimeSpan MaxShutdownBudget = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.Ordinal);
    private readonly ILogger<ServerClientPool> _logger;
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
        var nodeIds = new string[peers.Count];

        for (var i = 0; i < peers.Count; i++)
        {
            var peer = peers[i];
            RegisterPeer(peer, args);
            nodeIds[i] = peer.NodeId;
        }

        Array.Sort(nodeIds, StringComparer.Ordinal);
        _nodeIds = nodeIds;
        NodeIds = _nodeIds;
    }

    internal IReadOnlyCollection<string> NodeIds { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        // The peers drain in parallel under one budget, so a stuck peer cannot hold up host shutdown; channels are disposed either way.
        BeginDrain();
        var drains = new Task[_nodeIds.Length];
        for (var i = 0; i < _nodeIds.Length; i++)
            drains[i] = DisposePolicyAsync(_nodeIds[i]);

        try
        {
            await Task.WhenAll(drains).WaitAsync(_shutdownBudget, _timeProvider).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            ServerLog.ClientPoolDrainTimedOut(_logger, _shutdownBudget, BusyPeers(drains));
        }

        DisposeChannels();
    }

    public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => _cacheClients[nodeId];

    public GrpcChannel OpenChannel(string nodeId) => _channels[nodeId];

    public IServerCallPolicy PolicyFor(string nodeId) => _policies[nodeId];

    private static GrpcChannelOptions CreateChannelOptions(
        string nodeId,
        bool interNodeMtlsEnabled,
        MtlsCertificate? certificate,
        Func<string, HttpMessageHandler>? peerHandlerFactory,
        Func<MtlsCertificate?, string, HttpMessageHandler> ownedHandlerFactory)
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
                    ownedHandler = ownedHandlerFactory.Invoke(certificate, nodeId);
                    peerHandler = ownedHandler;
                }
            }
            else
            {
                ownedHandler = ownedHandlerFactory.Invoke(null, nodeId);
                peerHandler = ownedHandler;
            }

            var options = new GrpcChannelOptions
            {
                HttpHandler = peerHandler,

                // The channel disposes the handler the pool created; a peerHandlerFactory handler stays owned by the factory's caller.
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
        var busy = new List<string>(drains.Length);
        for (var i = 0; i < drains.Length; i++)
        {
            if (!drains[i].IsCompleted)
                busy.Add(_nodeIds[i]);
        }

        return string.Join(", ", busy);
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

    private async Task DisposePolicyAsync(string nodeId)
    {
        var failure = await _policies[nodeId].CaptureFailureAsync().ConfigureAwait(false);
        if (failure != null)
            ServerLog.ClientPoolPolicyDisposeFailed(_logger, failure, nodeId);
    }

    private void RegisterPeer(ServerPeer peer, ServerClientPoolArgs args)
    {
        var mtlsOptions = args.MtlsOptions ?? new MtlsOptions();
        var address = ClusterPeerChannelAddress.Resolve(peer, mtlsOptions, args.InterNodeMtlsEnabled);
        var ownedHandlerFactory = args.OwnedHandlerFactory ?? ServerGrpcEndpoints.CreateOwnedHandler;
        var options = CreateChannelOptions(peer.NodeId, args.InterNodeMtlsEnabled, args.Certificate, args.PeerHandlerFactory, ownedHandlerFactory);
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
        private static readonly List<SslApplicationProtocol> Http2PreferredProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11];

        /// <summary>Creates the handler a pool owns for one peer: mTLS when <paramref name="certificate" /> is supplied, plain HTTPS otherwise.</summary>
        /// <param name="certificate">Loaded cluster mTLS material, or <see langword="null" /> for plain HTTPS.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <returns>A handler owned by the caller.</returns>
        internal static SocketsHttpHandler CreateOwnedHandler(MtlsCertificate? certificate, string expectedPeerNodeId) =>
            certificate == null ? CreateChannelHandler() : CreateMtlsHandler(certificate, expectedPeerNodeId);

        /// <summary>Creates the default HTTP handler for HTTPS gRPC channels.</summary>
        /// <returns>A handler suitable for secure gRPC transport that opens more HTTP/2 connections once the stream limit is reached.</returns>
        private static SocketsHttpHandler CreateChannelHandler() => new()
        {
            EnableMultipleHttp2Connections = true,
        };

        /// <summary>Creates an outbound cluster mTLS HTTP handler that presents the local node certificate.</summary>
        /// <param name="certificate">Loaded cluster mTLS certificate.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <returns>A handler configured for internode mutual TLS.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="certificate" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when cluster mTLS certificate is not loaded.</exception>
        private static SocketsHttpHandler CreateMtlsHandler(MtlsCertificate certificate, string expectedPeerNodeId)
        {
            ArgumentNullException.ThrowIfNull(certificate);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);
            var missingMaterial = !certificate.Enabled || certificate.NodeCertificate == null || certificate.TrustAnchor == null;
            const string message = "Cluster mTLS certificate must be loaded before creating the outbound handler.";
            return missingMaterial ? throw new InvalidOperationException(message) : CreateMtlsHandler(certificate.NodeCertificate!, certificate.TrustAnchor!, expectedPeerNodeId);
        }

        /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
        /// <param name="clientCertificate">Client certificate presented to the peer.</param>
        /// <param name="trustAnchor">Configured cluster trust root.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <returns>A handler configured for internode mutual TLS.</returns>
        private static SocketsHttpHandler CreateMtlsHandler(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
        {
            ArgumentNullException.ThrowIfNull(clientCertificate);
            ArgumentNullException.ThrowIfNull(trustAnchor);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);

            return new SocketsHttpHandler
            {
                UseProxy = false,
                EnableMultipleHttp2Connections = true,
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
