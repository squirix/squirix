using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Threading;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Holds gRPC clients per peer and an execution policy (timeout/retry/concurrency) per peer.</summary>
[Immutable]
internal sealed class ClientPool : IClientPool
{
    private const int MaxReceiveMessageSizeBytes = 8 * 1024 * 1024;

    private const int MaxSendMessageSizeBytes = 8 * 1024 * 1024;

    private static readonly BootstrapConnectOptions DefaultConnectOptions = new(BootstrapConnectOptions.DefaultPerAttemptTimeout, BootstrapConnectOptions.DefaultOverallDeadline);

    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.OrdinalIgnoreCase);
    private readonly BootstrapConnectOptions _connectOptions;
    private readonly string[] _nodeIds;
    private readonly ConcurrentDictionary<string, ICallPolicy> _policies = new(StringComparer.OrdinalIgnoreCase);
    private readonly TimeProvider _timeProvider;
    private Task? _disposeTask;

    internal ClientPool(
        Peer[] peers,
        Func<string, ICallPolicy> policyFactory,
        HttpMessageHandler? handler = null,
        Interceptor? interceptor = null,
        CallCredentials? callCredentials = null,
        BootstrapConnectOptions? connectOptions = null,
        TimeProvider? timeProvider = null)
    {
        _connectOptions = connectOptions ?? DefaultConnectOptions;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _nodeIds = RegisterPeers(peers, policyFactory, handler, GrpcTransportEndpoints.CreateChannelHandler, interceptor, callCredentials);
        BootstrapNodeIds = _nodeIds;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClientPool" /> class whose per-peer HTTP handlers come from
    /// <paramref name="ownedHandlerFactory" /> (test seam for handler ownership).
    /// </summary>
    /// <param name="peers">Bootstrap peers.</param>
    /// <param name="policyFactory">Per-peer call policy factory.</param>
    /// <param name="ownedHandlerFactory">Creates one handler per peer in place of the default transport handler; the pool disposes each one.</param>
    internal ClientPool(Peer[] peers, Func<string, ICallPolicy> policyFactory, Func<HttpMessageHandler> ownedHandlerFactory)
    {
        _connectOptions = DefaultConnectOptions;
        _timeProvider = TimeProvider.System;
        _nodeIds = RegisterPeers(peers, policyFactory, null, ownedHandlerFactory, null, null);
        BootstrapNodeIds = _nodeIds;
    }

    internal IReadOnlyList<string> BootstrapNodeIds { get; }

    void IClientPool.BeginDrain() => BeginDrain();

    public ValueTask DisposeAsync() => new(AsyncLazyInitializer.EnsureStartedAsync(ref _disposeTask, this, static pool => pool.DisposeCoreAsync()));

    public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId) => _cacheClients[nodeId];

    public ICallPolicy PolicyFor(string nodeId) => _policies[nodeId];

    /// <summary>
    /// Connects to bootstrap endpoints and returns the first reachable node id in configuration order.
    /// Unreachable endpoints are skipped; startup fails only when no endpoint can be reached.
    /// After a primary peer connects, remaining peers use a short fail-fast connect budget.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The first reachable bootstrap node id.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no bootstrap endpoint is reachable.</exception>
    internal async ValueTask<string> WarmUpAsync(CancellationToken cancellationToken = default)
    {
        Exception? lastFailure = null;
        string? primaryNodeId = null;
        var failuresByNode = new Dictionary<string, Exception>(_nodeIds.Length, StringComparer.Ordinal);

        // Walk bootstrap peers in configuration order; the first reachable node becomes the primary session target.
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var id = _nodeIds[i];
            cancellationToken.ThrowIfCancellationRequested();
            if (!_channels.TryGetValue(id, out var channel))
                continue;

            // Primary peer uses the configured bootstrap deadline; secondary peers use a short fail-fast budget.
            var connectOptions = primaryNodeId == null ? _connectOptions : BootstrapConnectOptions.SecondaryPeerAfterPrimary;
            var failure = await WarmPeerAsync(channel, connectOptions, cancellationToken).ConfigureAwait(false);
            if (failure == null)
            {
                primaryNodeId ??= id;
                continue;
            }

            lastFailure = failure;
            failuresByNode[id] = failure;
        }

        if (primaryNodeId == null)
            throw lastFailure ?? new InvalidOperationException("No bootstrap endpoints are configured.");

        RecordSecondaryWarmupFailures(primaryNodeId, failuresByNode);
        return primaryNodeId;
    }

    private static void RecordSecondaryWarmupFailures(string primaryNodeId, Dictionary<string, Exception> failuresByNode)
    {
        // Unreachable secondary peers are tolerated once a primary is known; diagnostics record each skip.
        foreach (var pair in failuresByNode)
        {
            if (string.Equals(pair.Key, primaryNodeId, StringComparison.Ordinal))
                continue;

            ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped(pair.Key, pair.Value);
        }
    }

    /// <summary>Marks every peer policy as draining; a policy that fails is counted and does not stop the others.</summary>
    private void BeginDrain()
    {
#pragma warning disable CA1031 // Policies come from the pool's factory and are not restricted to known exception types; draining runs on the dispose path, which never throws.
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var nodeId = _nodeIds[i];
            try
            {
                _policies[nodeId].BeginDrain();
            }
            catch (Exception ex)
            {
                ClientPoolMetrics.AddPolicyDisposeFailure(nodeId, ex);
            }
        }
#pragma warning restore CA1031
    }

    private async Task DisposeCoreAsync()
    {
        // Best-effort shutdown: a failure of one peer must not leave the remaining peers undisposed, and disposal never throws. Failures
        // are counted per peer, stage and exception type because the client has no logging pipeline.
        BeginDrain();
#pragma warning disable CA1031 // Policies come from the pool's factory and channels dispose pool-created HTTP handlers; neither is restricted to known exception types.
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var nodeId = _nodeIds[i];
            try
            {
                await _policies[nodeId].DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ClientPoolMetrics.AddPolicyDisposeFailure(nodeId, ex);
            }
        }

        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var nodeId = _nodeIds[i];
            try
            {
                _channels[nodeId].Dispose();
                ClientPoolMetrics.AddDisposal();
            }
            catch (Exception ex)
            {
                ClientPoolMetrics.AddChannelDisposeFailure(nodeId, ex);
            }
        }
#pragma warning restore CA1031
    }

    private string[] RegisterPeers(
        Peer[] peers,
        Func<string, ICallPolicy> policyFactory,
        HttpMessageHandler? sharedHandler,
        Func<HttpMessageHandler> ownedHandlerFactory,
        Interceptor? interceptor,
        CallCredentials? callCredentials)
    {
        var ids = new string[peers.Length];

        for (var i = 0; i < peers.Length; i++)
        {
            var p = peers[i];
            GrpcTransportEndpoints.RequireHttps(p.Uri);
            var opts = new GrpcChannelOptions
            {
                Credentials = callCredentials == null ? null : ChannelCredentials.Create(new SslCredentials(), callCredentials),
                HttpHandler = sharedHandler ?? ownedHandlerFactory.Invoke(),

                // The pool owns and disposes the handlers it creates; a caller-supplied handler is shared by every channel and stays caller-owned.
                DisposeHttpClient = sharedHandler == null,
                MaxReceiveMessageSize = MaxReceiveMessageSizeBytes,
                MaxSendMessageSize = MaxSendMessageSizeBytes,
            };
            var channel = GrpcChannel.ForAddress(p.Uri, opts);
            var invoker = channel.CreateCallInvoker();
            if (interceptor != null)
                invoker = invoker.Intercept(interceptor);
            _channels[p.NodeId] = channel;
            _cacheClients[p.NodeId] = new SquirixCacheService.SquirixCacheServiceClient(invoker);
            _policies[p.NodeId] = policyFactory.Invoke(p.NodeId);
            ids[i] = p.NodeId;
        }

        return ids;
    }

    private async ValueTask<Exception?> WarmPeerAsync(GrpcChannel channel, BootstrapConnectOptions connectOptions, CancellationToken cancellationToken)
    {
        try
        {
            await GrpcChannelConnectWarmup.ConnectWithRetryAsync(channel, connectOptions, cancellationToken, _timeProvider).ConfigureAwait(false);
            ClientPoolMetrics.AddWarmup();
            return null;
        }
        catch (Exception ex) when (ex is RpcException or IOException or HttpRequestException or InvalidOperationException)
        {
            return ex;
        }
    }

    private static class GrpcChannelConnectWarmup
    {
        internal static async ValueTask ConnectWithRetryAsync(
            GrpcChannel channel,
            BootstrapConnectOptions options,
            CancellationToken cancellationToken,
            TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(channel);

            var time = timeProvider ?? TimeProvider.System;
            var deadlineUtc = time.GetUtcNow() + options.OverallDeadline;
            Exception? lastFailure = null;
            var attempt = 0;

            // Retry until the overall deadline; each attempt is bounded independently so one slow peer cannot consume the full budget.
            while (time.GetUtcNow() < deadlineUtc)
            {
                cancellationToken.ThrowIfCancellationRequested();
                attempt++;

                var remaining = deadlineUtc - time.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                    break;

                var attemptTimeout = remaining < options.PerAttemptTimeout ? remaining : options.PerAttemptTimeout;
                var failure = await ConnectOnceAsync(channel, attemptTimeout, cancellationToken).ConfigureAwait(false);
                if (failure == null)
                    return;

                lastFailure = failure;
                remaining = deadlineUtc - time.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                    break;

                var backoff = BackoffWithJitter(attempt, options);
                if (backoff > remaining)
                    backoff = remaining;

                // Never sleep past the overall connection deadline.
                await Task.Delay(backoff, time, cancellationToken).ConfigureAwait(false);
            }

            throw lastFailure ?? new InvalidOperationException("Failed to connect to endpoint within the configured deadline.");
        }

        private static TimeSpan BackoffWithJitter(int attempt, BootstrapConnectOptions options)
        {
            var pow = Math.Min(attempt - 1, 6);
            var cappedMs = Math.Min(options.MaxBackoff.TotalMilliseconds, options.BaseBackoff.TotalMilliseconds * Math.Pow(2, pow));
            var jitterFactor = 0.5 + (RandomNumberGenerator.GetInt32(0, 5000) / 10000.0);
            var finalMs = Math.Max(cappedMs * jitterFactor, Math.Min(50.0, cappedMs));
            return TimeSpan.FromMilliseconds(finalMs);
        }

        private static async ValueTask<Exception?> ConnectOnceAsync(GrpcChannel channel, TimeSpan attemptTimeout, CancellationToken cancellationToken)
        {
            // Linked CTS distinguishes caller cancellation from per-attempt connect timeouts.
            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            attemptCts.CancelAfter(attemptTimeout);

            try
            {
                await channel.ConnectAsync(attemptCts.Token).ConfigureAwait(false);
                return null;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // Attempt timeout: preserve the failure and retry with backoff until the overall deadline expires.
                return new InvalidOperationException("Failed to connect to endpoint within the per-attempt timeout.");
            }
            catch (HttpRequestException ex)
            {
                return ex;
            }
            catch (IOException ex)
            {
                return ex;
            }
            catch (RpcException ex)
            {
                return ex;
            }
        }
    }
}
