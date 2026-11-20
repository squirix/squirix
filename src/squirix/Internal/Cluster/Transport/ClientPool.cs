using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Threading;
using Squirix.Transport;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Holds binary clients per peer and an execution policy (timeout/retry/concurrency) per peer.</summary>
[Immutable]
internal sealed class ClientPool : IClientPool
{
    private static readonly BootstrapConnectOptions DefaultConnectOptions = new(BootstrapConnectOptions.DefaultPerAttemptTimeout, BootstrapConnectOptions.DefaultOverallDeadline);

    private readonly ConcurrentDictionary<string, NodeClient> _clients = new(StringComparer.OrdinalIgnoreCase);

    private readonly BootstrapConnectOptions _connectOptions;
    private readonly string[] _nodeIds;
    private readonly TimeProvider _timeProvider;
    private Task? _disposeTask;

    internal ClientPool(
        Peer[] peers,
        Func<string, ICallPolicy> policyFactory,
        TimeProvider timeProvider,
        HttpMessageHandler? handler = null,
        Func<CancellationToken, ValueTask<string>>? bearerTokenProvider = null,
        BootstrapConnectOptions? connectOptions = null)
    {
        _connectOptions = connectOptions ?? DefaultConnectOptions;
        _timeProvider = timeProvider;
        _nodeIds = RegisterPeers(peers, policyFactory, handler, bearerTokenProvider);
        BootstrapNodeIds = _nodeIds;
    }

    internal IReadOnlyList<string> BootstrapNodeIds { get; }

    void IClientPool.BeginDrain() => BeginDrain();

    public ValueTask DisposeAsync() => new(AsyncLazyInitializer.EnsureStartedAsync(ref _disposeTask, this, static pool => pool.DisposeCoreAsync()));

    public NodeClient ForNode(string nodeId) => _clients[nodeId];

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
            if (!_clients.TryGetValue(id, out var node))
                continue;

            // Primary peer uses the configured bootstrap deadline; secondary peers use a short fail-fast budget.
            var connectOptions = primaryNodeId == null ? _connectOptions : BootstrapConnectOptions.SecondaryPeerAfterPrimary;
            var failure = await WarmPeerAsync(node.Client, connectOptions, cancellationToken).ConfigureAwait(false);
            if (failure == null)
            {
                primaryNodeId ??= id;
                continue;
            }

            if (primaryNodeId == null && failure is SquirixTransportException { StatusCode: CacheTransportStatusCode.Unauthenticated })
                throw failure;

            lastFailure = failure;
            failuresByNode[id] = failure;
        }

        if (primaryNodeId == null)
            throw lastFailure ?? new InvalidOperationException("No bootstrap endpoints are configured.");

        RecordSecondaryWarmupFailures(primaryNodeId, failuresByNode);
        return primaryNodeId;
    }

    private static SslClientAuthenticationOptions? ReadTlsOptions(HttpMessageHandler? handler)
    {
        while (handler is DelegatingHandler { InnerHandler: { } inner })
            handler = inner;

        return (handler as SocketsHttpHandler)?.SslOptions;
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
                _clients[nodeId].Policy.BeginDrain();
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
                await _clients[nodeId].Policy.DisposeAsync().ConfigureAwait(false);
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
                await _clients[nodeId].Client.DisposeAsync().ConfigureAwait(false);
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
        HttpMessageHandler? handler,
        Func<CancellationToken, ValueTask<string>>? bearerTokenProvider)
    {
        var tlsOptions = ReadTlsOptions(handler);
        var ids = new string[peers.Length];
        for (var i = 0; i < peers.Length; i++)
        {
            var peer = peers[i];
            BinaryCacheClient.RequireHttps(peer.Uri);
            var policy = policyFactory.Invoke(peer.NodeId);
#pragma warning disable CA2000 // The pool owns clients published in its per-peer registry and disposes all of them.
            _clients[peer.NodeId] = new NodeClient(new BinaryCacheClient(peer.Uri, bearerTokenProvider, tlsOptions), policy);
#pragma warning restore CA2000
            ids[i] = peer.NodeId;
        }

        return ids;
    }

    private async ValueTask<Exception?> WarmPeerAsync(BinaryCacheClient channel, BootstrapConnectOptions connectOptions, CancellationToken cancellationToken)
    {
        try
        {
            await BinaryConnectWarmup.ConnectWithRetryAsync(channel, connectOptions, _timeProvider, cancellationToken).ConfigureAwait(false);
            ClientPoolMetrics.AddWarmup();
            return null;
        }
        catch (Exception ex) when (ex is SquirixTransportException or IOException or HttpRequestException or InvalidOperationException)
        {
            return ex;
        }
    }

    private static class BinaryConnectWarmup
    {
        internal static async ValueTask ConnectWithRetryAsync(
            BinaryCacheClient channel,
            BootstrapConnectOptions options,
            TimeProvider time,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(channel);

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
                var failure = await ConnectOnceAsync(channel, attemptTimeout, time, cancellationToken).ConfigureAwait(false);
                if (failure == null)
                    return;

                if (failure is SquirixTransportException { StatusCode: CacheTransportStatusCode.Unauthenticated or CacheTransportStatusCode.FailedPrecondition })
                    throw failure;
                lastFailure = failure;
                remaining = deadlineUtc - time.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                    break;

                var backoff = BackoffWithJitter(attempt);
                if (backoff > remaining)
                    backoff = remaining;

                // Never sleep past the overall connection deadline.
                await Task.Delay(backoff, time, cancellationToken).ConfigureAwait(false);
            }

            throw lastFailure ?? new InvalidOperationException("Failed to connect to endpoint within the configured deadline.");
        }

        private static TimeSpan BackoffWithJitter(int attempt)
        {
            var pow = Math.Min(attempt - 1, 6);
            const double baseBackoffMs = 200;
            const double maxBackoffMs = 2000;
            var cappedMs = Math.Min(maxBackoffMs, baseBackoffMs * Math.Pow(2, pow));
            var jitterFactor = 0.5 + (RandomNumberGenerator.GetInt32(0, 5000) / 10000.0);
            var finalMs = Math.Max(cappedMs * jitterFactor, Math.Min(50.0, cappedMs));
            return TimeSpan.FromMilliseconds(finalMs);
        }

        private static async ValueTask<Exception?> ConnectOnceAsync(BinaryCacheClient channel, TimeSpan attemptTimeout, TimeProvider time, CancellationToken cancellationToken)
        {
            // Linked CTS distinguishes caller cancellation from per-attempt connect timeouts.
            using var attemptTimer = new CancellationTokenSource(attemptTimeout, time);
            using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, attemptTimer.Token);

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
            catch (SquirixTransportException ex)
            {
                return ex;
            }
        }
    }
}
