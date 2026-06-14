using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Limits;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Holds gRPC clients per peer and an execution policy per peer.
/// </summary>
internal sealed class ClientPool : IClientPool
{
    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.OrdinalIgnoreCase);
    private readonly BootstrapConnectOptions _connectOptions;
    private readonly ConcurrentDictionary<string, ICallPolicy> _policies = new(StringComparer.OrdinalIgnoreCase);
    private int _disposed;
    private volatile bool _draining;

    [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "GrpcChannel disposes HttpHandler when the channel is disposed.")]
    public ClientPool(
        IEnumerable<Peer> peers,
        Func<string, ICallPolicy> policyFactory,
        Func<string, HttpMessageHandler>? peerHandlerFactory = null,
        Interceptor? interceptor = null,
        BootstrapConnectOptions? connectOptions = null,
        MtlsOptions? mtlsOptions = null,
        MtlsCertificateMaterial? mtlsMaterial = null,
        bool interNodeMtlsEnabled = false,
        Interceptor? internalOwnerInterceptor = null)
    {
        _connectOptions = connectOptions ?? new BootstrapConnectOptions(BootstrapConnectOptions.DefaultPerAttemptTimeout, BootstrapConnectOptions.DefaultOverallDeadline);
        var peerList = peers as Peer[] ?? [.. peers];
        var nodeIds = new string[peerList.Length];
        var resolvedMtlsOptions = mtlsOptions ?? new MtlsOptions();

        for (var i = 0; i < peerList.Length; i++)
        {
            var p = peerList[i];
            var address = ClusterPeerChannelAddress.Resolve(p, resolvedMtlsOptions, interNodeMtlsEnabled);
            HttpMessageHandler? peerHandler = null;
            if (interNodeMtlsEnabled)
            {
                if (mtlsMaterial is not { Enabled: true })
                    throw new InvalidOperationException("Cluster mTLS material must be loaded for inter-node transport.");

                peerHandler = peerHandlerFactory?.Invoke(p.NodeId) ?? GrpcTransportEndpoints.CreateMtlsHandler(mtlsMaterial, p.NodeId);
            }

            var opts = new GrpcChannelOptions
            {
                HttpHandler = peerHandler ?? GrpcTransportEndpoints.CreateChannelHandler(),
                MaxReceiveMessageSize = SquirixEntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = SquirixEntryLimits.GrpcMaxSendMessageSizeBytes,
            };
            var channel = GrpcChannel.ForAddress(address, opts);
            var invoker = channel.CreateCallInvoker();
            if (internalOwnerInterceptor is not null)
                invoker = invoker.Intercept(internalOwnerInterceptor);
            if (interceptor is not null)
                invoker = invoker.Intercept(interceptor);
            _channels[p.NodeId] = channel;
            _cacheClients[p.NodeId] = new SquirixCacheService.SquirixCacheServiceClient(invoker);
            _policies[p.NodeId] = policyFactory.Invoke(p.NodeId);
            nodeIds[i] = p.NodeId;
        }

        Array.Sort(nodeIds, StringComparer.Ordinal);
        NodeIds = nodeIds;
    }

    public IReadOnlyCollection<string> NodeIds { get; }

    internal int ActiveClientCount => _cacheClients.Count;

    internal bool IsDraining => _draining;

    public void BeginDrain()
    {
        _draining = true;
        foreach (var policy in _policies.Values)
            policy.BeginDrain();
    }

    public async ValueTask WarmUpAsync(CancellationToken cancellationToken = default)
    {
        foreach (var entry in _channels)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await GrpcChannelConnectWarmup.ConnectWithRetryAsync(entry.Value, entry.Key, _connectOptions, cancellationToken).ConfigureAwait(false);
            ClientPoolMetrics.AddWarmup();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        BeginDrain();
        foreach (var item in _policies)
        {
            try
            {
                await item.Value.DisposeAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // Best-effort drain.
            }
            catch (IOException)
            {
                // Best-effort drain.
            }
        }

        foreach (var ch in _channels)
        {
            try
            {
                ch.Value.Dispose();
                ClientPoolMetrics.AddDisposal();
            }
            catch (ObjectDisposedException)
            {
                // Best-effort drain.
            }
            catch (IOException)
            {
                // Best-effort drain.
            }
        }
    }

    public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => _cacheClients[nodeId];

    public ICallPolicy PolicyFor(string nodeId) => _policies[nodeId];
}
