using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
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
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Holds gRPC clients per peer and an execution policy per peer.</summary>
[Mutable]
internal sealed class ServerClientPool : IServerClientPool
{
    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.Ordinal);
    private readonly string[] _nodeIds;
    private readonly ConcurrentDictionary<string, IServerCallPolicy> _policies = new(StringComparer.Ordinal);
    private readonly ILogger? _logger;
    private int _disposed;

    internal ServerClientPool(IReadOnlyList<ServerPeer> peers, ServerClientPoolArgs args)
    {
        ArgumentNullException.ThrowIfNull(args);
        _logger = args.Logger;
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

        BeginDrain();
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var nodeId = _nodeIds[i];
            try
            {
                await _policies[nodeId].DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is ObjectDisposedException or IOException)
            {
                if (_logger != null)
                    LogManager.ClientPoolPolicyDisposeFailed(_logger, exception, nodeId);
            }
        }

        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var nodeId = _nodeIds[i];
            try
            {
                _channels[nodeId].Dispose();
                ServerClientPoolMetrics.AddDisposal();
            }
            catch (Exception exception) when (exception is ObjectDisposedException or IOException)
            {
                if (_logger != null)
                    LogManager.ClientPoolChannelDisposeFailed(_logger, exception, nodeId);
            }
        }
    }

    public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => _cacheClients[nodeId];

    public IServerCallPolicy PolicyFor(string nodeId) => _policies[nodeId];

    [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "GrpcChannel disposes HttpHandler when the channel is disposed.")]
    private static GrpcChannelOptions CreateChannelOptions(
        string nodeId,
        bool interNodeMtlsEnabled,
        MtlsCertificateMaterial? mtlsMaterial,
        Func<string, HttpMessageHandler>? peerHandlerFactory)
    {
        var peerHandler = interNodeMtlsEnabled switch
        {
            true when mtlsMaterial is not { Enabled: true } => throw new InvalidOperationException("Cluster mTLS material must be loaded for inter-node transport."),
            true => peerHandlerFactory?.Invoke(nodeId) ?? ServerGrpcEndpoints.CreateMtlsHandler(mtlsMaterial, nodeId),
            _ => null,
        };

        return new GrpcChannelOptions
        {
            HttpHandler = peerHandler ?? ServerGrpcEndpoints.CreateChannelHandler(),
            MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
            MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
        };
    }

    private void BeginDrain()
    {
        for (var i = 0; i < _nodeIds.Length; i++)
            _policies[_nodeIds[i]].BeginDrain();
    }

    private void RegisterPeer(ServerPeer peer, ServerClientPoolArgs args)
    {
        var mtlsOptions = args.MtlsOptions ?? new MtlsOptions();
        var address = ClusterPeerChannelAddress.Resolve(peer, mtlsOptions, args.InterNodeMtlsEnabled);
        var channel = GrpcChannel.ForAddress(address, CreateChannelOptions(peer.NodeId, args.InterNodeMtlsEnabled, args.MtlsMaterial, args.PeerHandlerFactory));
        var invoker = channel.CreateCallInvoker();
        if (args.InternalOwnerInterceptor != null)
            invoker = invoker.Intercept(args.InternalOwnerInterceptor);
        if (args.Interceptor != null)
            invoker = invoker.Intercept(args.Interceptor);

        _channels[peer.NodeId] = channel;
        _cacheClients[peer.NodeId] = new SquirixCacheService.SquirixCacheServiceClient(invoker);
        _policies[peer.NodeId] = args.PolicyFactory.Invoke(peer.NodeId);
    }

    /// <summary>Resolves gRPC channel addresses for inter-node cluster transport.</summary>
    private static class ClusterPeerChannelAddress
    {
        /// <summary>Resolves the gRPC endpoint used for inter-node cluster calls.</summary>
        /// <param name="peer">Configured cluster peer.</param>
        /// <param name="mtlsOptions">Cluster mTLS options for the local node.</param>
        /// <param name="interNodeMtlsEnabled">Whether inter-node mTLS transport is active.</param>
        /// <returns>The HTTPS gRPC address for pooled cluster clients.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="peer" /> or <paramref name="mtlsOptions" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when inter-node mTLS is enabled but the internal listen port or peer URI is invalid.</exception>
        internal static Uri Resolve(ServerPeer peer, MtlsOptions mtlsOptions, bool interNodeMtlsEnabled)
        {
            ArgumentNullException.ThrowIfNull(peer);
            ArgumentNullException.ThrowIfNull(mtlsOptions);

            if (!interNodeMtlsEnabled)
                return peer.Uri;

            if (peer.InterNodeUri is { } uri)
                return uri;

            if (mtlsOptions.InternalListenPort <= 0)
                throw new InvalidOperationException("Cluster mTLS internal listen port must be configured for inter-node transport.");

            var primaryUri = peer.Uri;
            if (!primaryUri.IsAbsoluteUri)
                throw new InvalidOperationException("Cluster peer URI is invalid.");

            return new UriBuilder(primaryUri.Scheme, primaryUri.Host, mtlsOptions.InternalListenPort).Uri;
        }
    }

    /// <summary>Validates and configures gRPC transport endpoints for server-to-server transport.</summary>
    private static class ServerGrpcEndpoints
    {
        private static readonly List<SslApplicationProtocol> Http2PreferredProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11];

        /// <summary>Creates the default HTTP handler for HTTPS gRPC channels.</summary>
        /// <returns>A handler suitable for secure gRPC transport.</returns>
        internal static SocketsHttpHandler CreateChannelHandler() => new();

        /// <summary>Creates an outbound cluster mTLS HTTP handler that presents the local node certificate.</summary>
        /// <param name="material">Loaded cluster mTLS certificate material.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <returns>A handler configured for inter-node mutual TLS.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="material" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown when cluster mTLS material is not loaded.</exception>
        internal static SocketsHttpHandler CreateMtlsHandler(MtlsCertificateMaterial material, string expectedPeerNodeId)
        {
            ArgumentNullException.ThrowIfNull(material);
            ArgumentException.ThrowIfNullOrWhiteSpace(expectedPeerNodeId);
            if (!material.Enabled || material.NodeCertificate == null || material.TrustAnchor == null)
                throw new InvalidOperationException("Cluster mTLS material must be loaded before creating the outbound handler.");

            return CreateMtlsHandler(material.NodeCertificate, material.TrustAnchor, expectedPeerNodeId);
        }

        /// <summary>Creates an outbound cluster mTLS HTTP handler with explicit client certificate material.</summary>
        /// <param name="clientCertificate">Client certificate presented to the peer.</param>
        /// <param name="trustAnchor">Configured cluster trust root.</param>
        /// <param name="expectedPeerNodeId">Configured cluster node identifier for the remote peer.</param>
        /// <returns>A handler configured for inter-node mutual TLS.</returns>
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
        /// <returns><see langword="true" /> when the certificate is trusted for inter-node traffic.</returns>
        private static bool ValidatePeerServerCertificate(X509Certificate? serverCertificate, X509Certificate2 trustAnchor, string expectedPeerNodeId)
        {
            if (serverCertificate == null)
                return false;

            using var certificate = new X509Certificate2(serverCertificate);
            return MtlsClientCertificateValidator.ValidateForExpectedNodeId(certificate, trustAnchor, expectedPeerNodeId);
        }
    }
}
