using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Limits;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>
/// Holds gRPC clients per peer and an execution policy (timeout/retry/concurrency) per peer.
/// </summary>
internal sealed class ClientPool : IClientPool
{
    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new();

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new();
    private readonly BootstrapConnectOptions _connectOptions;
    private readonly ConcurrentDictionary<string, ICallPolicy> _policies = new();
    private int _disposed;

    public ClientPool(
        IEnumerable<Peer> peers,
        Func<string, ICallPolicy> policyFactory,
        HttpMessageHandler? handler = null,
        Interceptor? interceptor = null,
        BootstrapConnectOptions? connectOptions = null)
    {
        _connectOptions = connectOptions ?? new BootstrapConnectOptions(BootstrapConnectOptions.DefaultPerAttemptTimeout, BootstrapConnectOptions.DefaultOverallDeadline);
        var peerList = peers as Peer[] ?? [.. peers];
        var nodeIds = new string[peerList.Length];

        for (var i = 0; i < peerList.Length; i++)
        {
            var p = peerList[i];
            GrpcTransportEndpoints.RequireHttps(p.Url);
            var opts = new GrpcChannelOptions
            {
                HttpHandler = handler ?? GrpcTransportEndpoints.CreateChannelHandler(),
                MaxReceiveMessageSize = SquirixClientGrpcLimits.MaxReceiveMessageSizeBytes,
                MaxSendMessageSize = SquirixClientGrpcLimits.MaxSendMessageSizeBytes,
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

        BootstrapNodeIds = [.. nodeIds];
    }

    public IReadOnlyList<string> BootstrapNodeIds { get; }

    internal int ActiveClientCount => _cacheClients.Count;

    /// <summary>
    /// Connects to bootstrap endpoints and returns the first reachable node id in configuration order.
    /// Unreachable endpoints are skipped; startup fails only when no endpoint can be reached.
    /// After a primary peer connects, remaining peers use a short fail-fast connect budget.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The first reachable bootstrap node id.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no bootstrap endpoint is reachable.</exception>
    public async ValueTask<string> WarmUpAsync(CancellationToken cancellationToken = default)
    {
        Exception? lastFailure = null;
        string? primaryNodeId = null;
        var failuresByNode = new Dictionary<string, Exception>(BootstrapNodeIds.Count, StringComparer.Ordinal);

        foreach (var nodeId in BootstrapNodeIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_channels.TryGetValue(nodeId, out var channel))
                continue;

            var connectOptions = primaryNodeId is null
                ? _connectOptions
                : BootstrapConnectOptions.SecondaryPeerAfterPrimary;

            try
            {
                await GrpcChannelConnectWarmup.ConnectWithRetryAsync(channel, nodeId, connectOptions, cancellationToken).ConfigureAwait(false);
                ClientPoolMetrics.AddWarmup();
                primaryNodeId ??= nodeId;
            }
            catch (RpcException ex)
            {
                lastFailure = ex;
                failuresByNode[nodeId] = ex;
            }
            catch (IOException ex)
            {
                lastFailure = ex;
                failuresByNode[nodeId] = ex;
            }
            catch (HttpRequestException ex)
            {
                lastFailure = ex;
                failuresByNode[nodeId] = ex;
            }
            catch (InvalidOperationException ex)
            {
                lastFailure = ex;
                failuresByNode[nodeId] = ex;
            }
        }

        if (primaryNodeId is null)
            throw lastFailure ?? new InvalidOperationException("No bootstrap endpoints are configured.");
        foreach (var pair in failuresByNode)
        {
            if (string.Equals(pair.Key, primaryNodeId, StringComparison.Ordinal))
                continue;

            ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped(pair.Key, pair.Value);
        }

        return primaryNodeId;
    }

    public void BeginDrain()
    {
        foreach (var policy in _policies.Values)
            policy.BeginDrain();
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
                // Best-effort drain: one failing policy dispose must not block disposal of other peers.
            }
            catch (IOException)
            {
                // Best-effort drain: one failing policy dispose must not block disposal of other peers.
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
                // Best-effort drain: channel disposal failures are suppressed so all peers are still attempted.
            }
            catch (IOException)
            {
                // Best-effort drain: channel disposal failures are suppressed so all peers are still attempted.
            }
        }
    }

    public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => _cacheClients[nodeId];

    public ICallPolicy PolicyFor(string nodeId) => _policies[nodeId];
}
