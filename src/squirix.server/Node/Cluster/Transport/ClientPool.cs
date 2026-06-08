using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Squirix.Server.Limits;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Node.Cluster.Reliability;
using Squirix.Server.Node.Observability;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Cluster.Transport;

/// <summary>
/// Holds gRPC clients per peer and an execution policy per peer.
/// </summary>
internal sealed class ClientPool : IClientPool
{
    private const int ConnectTimeoutMs = 5_000;
    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new();

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new();
    private readonly ConcurrentDictionary<string, ICallPolicy> _policies = new();
    private int _disposed;
    private volatile bool _draining;

    public ClientPool(IEnumerable<Peer> peers, Func<string, ICallPolicy> policyFactory, HttpMessageHandler? handler = null, Interceptor? interceptor = null)
    {
        var peerList = peers as Peer[] ?? [.. peers];
        var nodeIds = new string[peerList.Length];

        for (var i = 0; i < peerList.Length; i++)
        {
            var p = peerList[i];
            GrpcTransportEndpoints.RequireHttps(p.Url);
            var opts = new GrpcChannelOptions
            {
                HttpHandler = handler ?? GrpcTransportEndpoints.CreateChannelHandler(),
                MaxReceiveMessageSize = SquirixEntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = SquirixEntryLimits.GrpcMaxSendMessageSizeBytes,
            };
            var channel = GrpcChannel.ForAddress(p.Url, opts);
            var invoker = channel.CreateCallInvoker();
            if (interceptor is not null)
                invoker = invoker.Intercept(interceptor);
            _channels[p.NodeId] = channel;
            _cacheClients[p.NodeId] = new SquirixCacheService.SquirixCacheServiceClient(invoker);
            _policies[p.NodeId] = policyFactory.Invoke(p.NodeId);
            nodeIds[i] = p.NodeId;
        }

        Array.Sort(nodeIds, StringComparer.Ordinal);
        NodeIds = nodeIds;
        ClientPoolMetrics.RegisterObservers(() => _cacheClients.Count, () => _channels.Count, () => _draining);
    }

    public IReadOnlyCollection<string> NodeIds { get; }

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
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(ConnectTimeoutMs);

            try
            {
                await entry.Value.ConnectAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new InvalidOperationException($"Failed to connect to endpoint '{entry.Key}' within {ConnectTimeoutMs}ms.");
            }

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
            catch
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
            catch
            {
                // Best-effort drain.
            }
        }
    }

    public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => _cacheClients[nodeId];

    public ICallPolicy PolicyFor(string nodeId) => _policies[nodeId];
}
