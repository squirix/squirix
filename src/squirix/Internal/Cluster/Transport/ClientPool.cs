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
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Holds gRPC clients per peer and an execution policy (timeout/retry/concurrency) per peer.</summary>
internal sealed class ClientPool : IClientPool
{
    private const int MaxReceiveMessageSizeBytes = 8 * 1024 * 1024;

    private const int MaxSendMessageSizeBytes = 8 * 1024 * 1024;

    private readonly ConcurrentDictionary<string, SquirixCacheService.SquirixCacheServiceClient> _cacheClients = new(StringComparer.OrdinalIgnoreCase);

    private readonly ConcurrentDictionary<string, GrpcChannel> _channels = new(StringComparer.OrdinalIgnoreCase);
    private readonly BootstrapConnectOptions _connectOptions;
    private readonly string[] _nodeIds;
    private readonly ConcurrentDictionary<string, ICallPolicy> _policies = new(StringComparer.OrdinalIgnoreCase);
    private readonly string[] _nodeIds;
    private readonly TimeProvider _timeProvider;
    private int _disposed;

    public ClientPool(
        Peer[] peers,
        Func<string, ICallPolicy> policyFactory,
        HttpMessageHandler? handler = null,
        Interceptor? interceptor = null,
        CallCredentials? callCredentials = null,
        BootstrapConnectOptions? connectOptions = null,
        TimeProvider? timeProvider = null)
    {
        _connectOptions = connectOptions ?? new BootstrapConnectOptions(BootstrapConnectOptions.DefaultPerAttemptTimeout, BootstrapConnectOptions.DefaultOverallDeadline);
        _timeProvider = timeProvider ?? TimeProvider.System;
        var ids = new string[peers.Length];

        for (var i = 0; i < peers.Length; i++)
        {
            var p = peers[i];
            GrpcTransportEndpoints.RequireHttps(p.Uri);
            var opts = new GrpcChannelOptions
            {
                Credentials = callCredentials is null ? null : ChannelCredentials.Create(new SslCredentials(), callCredentials),
                HttpHandler = handler ?? GrpcTransportEndpoints.CreateChannelHandler(),
                MaxReceiveMessageSize = MaxReceiveMessageSizeBytes,
                MaxSendMessageSize = MaxSendMessageSizeBytes,
            };
            var channel = GrpcChannel.ForAddress(p.Uri, opts);
            var invoker = channel.CreateCallInvoker();
            if (interceptor is not null)
                invoker = invoker.Intercept(interceptor);
            _channels[p.NodeId] = channel;
            _cacheClients[p.NodeId] = new SquirixCacheService.SquirixCacheServiceClient(invoker);
            _policies[p.NodeId] = policyFactory.Invoke(p.NodeId);
            ids[i] = p.NodeId;
        }

        _nodeIds = ids;
        BootstrapNodeIds = _nodeIds;
    }

    internal IReadOnlyList<string> BootstrapNodeIds { get; }

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
        var failuresByNode = new Dictionary<string, Exception>(_nodeIds.Length, StringComparer.Ordinal);

        for (var i = 0; i < _nodeIds.Length; i++)
        {
            var id = _nodeIds[i];
            cancellationToken.ThrowIfCancellationRequested();
            if (!_channels.TryGetValue(id, out var channel))
                continue;

            var connectOptions = primaryNodeId is null ? _connectOptions : BootstrapConnectOptions.SecondaryPeerAfterPrimary;

            try
            {
                await GrpcChannelConnectWarmup.ConnectWithRetryAsync(channel, id, connectOptions, cancellationToken, _timeProvider).ConfigureAwait(false);
                ClientPoolMetrics.AddWarmup();
                primaryNodeId ??= id;
            }
            catch (RpcException ex)
            {
                lastFailure = ex;
                failuresByNode[id] = ex;
            }
            catch (IOException ex)
            {
                lastFailure = ex;
                failuresByNode[id] = ex;
            }
            catch (HttpRequestException ex)
            {
                lastFailure = ex;
                failuresByNode[id] = ex;
            }
            catch (InvalidOperationException ex)
            {
                lastFailure = ex;
                failuresByNode[id] = ex;
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
        for (var i = 0; i < _nodeIds.Length; i++)
            _policies[_nodeIds[i]].BeginDrain();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        BeginDrain();
        for (var i = 0; i < _nodeIds.Length; i++)
        {
            try
            {
                await _policies[_nodeIds[i]].DisposeAsync().ConfigureAwait(false);
            }
            catch (ObjectDisposedException)
            {
                // Best-effort drain: one failing policy dispose must not block disposal of other peers.
            }
            catch (IOException)
            {
                // Best-effort drain: one failing policy dispose must not block disposal of other peers.
            }

        for (var i = 0; i < _nodeIds.Length; i++)
        {
            try
            {
                _channels[_nodeIds[i]].Dispose();
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
            var connectOptions = primaryNodeId is null ? _connectOptions : BootstrapConnectOptions.SecondaryPeerAfterPrimary;
            var failure = await TryWarmPeerAsync(channel, id, connectOptions, cancellationToken).ConfigureAwait(false);
            if (failure is null)
            {
                primaryNodeId ??= id;
                continue;
            }

            lastFailure = failure;
            failuresByNode[id] = failure;
        }

        if (primaryNodeId is null)
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

    private void BeginDrain()
    {
        for (var i = 0; i < _nodeIds.Length; i++)
            _policies[_nodeIds[i]].BeginDrain();
    }

    private async ValueTask<Exception?> TryWarmPeerAsync(GrpcChannel channel, string id, BootstrapConnectOptions connectOptions, CancellationToken cancellationToken)
    {
        try
        {
            await GrpcChannelConnectWarmup.ConnectWithRetryAsync(channel, id, connectOptions, cancellationToken, _timeProvider).ConfigureAwait(false);
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
            string endpointName,
            BootstrapConnectOptions options,
            CancellationToken cancellationToken,
            TimeProvider? timeProvider = null)
        {
            ArgumentNullException.ThrowIfNull(channel);
            ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);

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
                var failure = await TryConnectOnceAsync(channel, attemptTimeout, cancellationToken).ConfigureAwait(false);
                if (failure is null)
                    return;

                lastFailure = failure;
                remaining = deadlineUtc - time.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                    break;

                var backoff = BackoffWithJitter(attempt, options);
                if (backoff > remaining)
                    backoff = remaining;

                // Never sleep past the overall connect deadline.
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

        private static async ValueTask<Exception?> TryConnectOnceAsync(GrpcChannel channel, TimeSpan attemptTimeout, CancellationToken cancellationToken)
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
