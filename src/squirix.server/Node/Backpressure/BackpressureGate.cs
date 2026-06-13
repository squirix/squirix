using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Backpressure;

internal sealed class BackpressureGate : IBackpressureGate, IDisposable
{
    private readonly ConcurrentDictionary<string, ClientState> _clients = new(StringComparer.Ordinal);
    private readonly RateLimiter? _nodeRateLimiter;
    private readonly IDisposable _observerRegistration;
    private readonly BackpressureOptions _options;
    private readonly SemaphoreSlim _slots;
    private bool _disposed;
    private int _inFlight;
    private int _queueDepth;

    public BackpressureGate(BackpressureOptions options)
    {
        _options = options;
        _options.Validate();
        _slots = new SemaphoreSlim(_options.MaxInFlight, _options.MaxInFlight);
        _nodeRateLimiter = RateLimiter.Create(_options.NodeRateLimitPerSecond, _options.NodeRateLimitBurst);
        _observerRegistration = BackpressureMetrics.RegisterObservers(() => Volatile.Read(ref _inFlight), () => Volatile.Read(ref _queueDepth), () => _clients.Count);
    }

    public async ValueTask<(BackpressureDecision Decision, BackpressureLease Lease)> AcquireAsync(
        string transport,
        string operation,
        string clientId,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(transport);
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        var disabledResult = TryAcquireWhenDisabled(transport, operation);
        if (disabledResult.HasValue)
            return disabledResult.Value;

        cancellationToken.ThrowIfCancellationRequested();
        var client = _clients.GetOrAdd(clientId, static (_, options) => new ClientState(options), _options);

        var nodeRateLimitReject = TryRejectByNodeRateLimit(transport, operation);
        if (nodeRateLimitReject.HasValue)
            return nodeRateLimitReject.Value;

        var clientRateLimitReject = TryRejectByClientRateLimit(transport, operation, client);
        if (clientRateLimitReject.HasValue)
            return clientRateLimitReject.Value;

        var inFlight = Volatile.Read(ref _inFlight);
        var queueDepth = Volatile.Read(ref _queueDepth);
        var hardThresholdReject = TryRejectByHardThreshold(transport, operation, inFlight, queueDepth);
        if (hardThresholdReject.HasValue)
            return hardThresholdReject.Value;

        if (inFlight >= _options.SlowdownThreshold)
            await ApplySlowdownAsync(transport, operation, inFlight, cancellationToken).ConfigureAwait(false);

        var clientConcurrencyReject = TryRejectByPerClientConcurrency(transport, operation, clientId, client);
        return clientConcurrencyReject ?? await AcquireFromSlotOrQueueAsync(transport, operation, clientId, client, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _observerRegistration.Dispose();
        _slots.Dispose();
    }

    private async ValueTask<(BackpressureDecision Decision, BackpressureLease Lease)> AcquireFromSlotOrQueueAsync(
        string transport,
        string operation,
        string clientId,
        ClientState client,
        CancellationToken cancellationToken)
    {
        return await _slots.WaitAsync(0, cancellationToken).ConfigureAwait(false) ? (BackpressureDecision.Accepted(), AcquireLease(clientId, client))
            : await WaitInQueueAsync(transport, operation, clientId, client, cancellationToken).ConfigureAwait(false);
    }

    private BackpressureLease AcquireLease(string clientId, ClientState client)
    {
        _ = Interlocked.Increment(ref _inFlight);
        _ = Interlocked.Increment(ref client.InFlightRef);
        return new BackpressureLease(() => Release(clientId, client));
    }

    private async Task ApplySlowdownAsync(string transport, string operation, int inFlight, CancellationToken cancellationToken)
    {
        var window = Math.Max(1, _options.RejectThreshold - _options.SlowdownThreshold);
        var relative = Math.Clamp((double)(inFlight - _options.SlowdownThreshold + 1) / window, 0d, 1d);
        var delay = TimeSpan.FromMilliseconds(_options.MaxSlowdownDelay.TotalMilliseconds * relative);
        if (delay <= TimeSpan.Zero)
            return;

        BackpressureMetrics.AddSlowdown(transport, operation);
        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
    }

    private void Release(string clientId, ClientState client)
    {
        _ = Interlocked.Decrement(ref client.InFlightRef);
        _ = Interlocked.Decrement(ref _inFlight);
        _ = _slots.Release();
        TryTrimClient(clientId, client);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private (BackpressureDecision Decision, BackpressureLease Lease)? TryAcquireWhenDisabled(string transport, string operation)
    {
        if (_options.Enabled)
            return null;

        BackpressureMetrics.AddBypass(transport, operation);
        return (BackpressureDecision.Accepted(), BackpressureLease.Empty);
    }

    private (BackpressureDecision Decision, BackpressureLease Lease)? TryRejectByClientRateLimit(string transport, string operation, ClientState client)
    {
        if (!_options.Enabled || client.RateLimiter is null || client.RateLimiter.TryAcquire())
            return null;

        BackpressureMetrics.AddRateLimitReject(transport, operation, "client");
        BackpressureMetrics.AddReject(transport, operation, "client_rate_limit");
        return (BackpressureDecision.Rejected("client_rate_limit"), BackpressureLease.Empty);
    }

    private (BackpressureDecision Decision, BackpressureLease Lease)? TryRejectByHardThreshold(string transport, string operation, int inFlight, int queueDepth)
    {
        if (inFlight < _options.RejectThreshold || queueDepth <= 0)
            return null;

        BackpressureMetrics.AddReject(transport, operation, "hard_threshold");
        return (BackpressureDecision.Rejected("hard_threshold"), BackpressureLease.Empty);
    }

    private (BackpressureDecision Decision, BackpressureLease Lease)? TryRejectByNodeRateLimit(string transport, string operation)
    {
        if (_nodeRateLimiter is null || _nodeRateLimiter.TryAcquire())
            return null;

        BackpressureMetrics.AddRateLimitReject(transport, operation, "node");
        BackpressureMetrics.AddReject(transport, operation, "node_rate_limit");
        return (BackpressureDecision.Rejected("node_rate_limit"), BackpressureLease.Empty);
    }

    private (BackpressureDecision Decision, BackpressureLease Lease)? TryRejectByPerClientConcurrency(string transport, string operation, string clientId, ClientState client)
    {
        if (_options.PerClientMaxInFlight is not { } perClientMaxInFlight || client.InFlight < perClientMaxInFlight)
            return null;

        var queuedForClient = Interlocked.Increment(ref client.QueueDepthRef);
        try
        {
            var maxClientQueue = _options.PerClientMaxQueue ?? _options.MaxQueue;
            if (queuedForClient > maxClientQueue)
            {
                BackpressureMetrics.AddReject(transport, operation, "client_queue_full");
                return (BackpressureDecision.Rejected("client_queue_full"), BackpressureLease.Empty);
            }

            BackpressureMetrics.AddReject(transport, operation, "client_concurrency_limit");
            return (BackpressureDecision.Rejected("client_concurrency_limit"), BackpressureLease.Empty);
        }
        finally
        {
            _ = Interlocked.Decrement(ref client.QueueDepthRef);
            TryTrimClient(clientId, client);
        }
    }

    private void TryTrimClient(string clientId, ClientState client)
    {
        if (client.InFlight != 0 || client.QueueDepth != 0 || client.RateLimiter?.HasRecentActivity == true)
            return;

        _ = _clients.TryRemove(new KeyValuePair<string, ClientState>(clientId, client));
    }

    private async ValueTask<(BackpressureDecision Decision, BackpressureLease Lease)> WaitInQueueAsync(
        string transport,
        string operation,
        string clientId,
        ClientState client,
        CancellationToken cancellationToken)
    {
        var queued = Interlocked.Increment(ref _queueDepth);
        if (queued > _options.MaxQueue)
        {
            _ = Interlocked.Decrement(ref _queueDepth);
            BackpressureMetrics.AddReject(transport, operation, "queue_full");
            return (BackpressureDecision.Rejected("queue_full"), BackpressureLease.Empty);
        }

        var started = Stopwatch.GetTimestamp();
        try
        {
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCts.CancelAfter(_options.MaxQueueWait);

            try
            {
                await _slots.WaitAsync(timeoutCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                BackpressureMetrics.AddQueueTimeout(transport, operation);
                BackpressureMetrics.AddReject(transport, operation, "queue_wait_timeout");
                return (BackpressureDecision.Rejected("queue_wait_timeout"), BackpressureLease.Empty);
            }

            var queueWait = Stopwatch.GetElapsedTime(started);
            BackpressureMetrics.RecordQueueWait(queueWait, transport, operation);
            return (BackpressureDecision.Accepted(), AcquireLease(clientId, client));
        }
        catch (OperationCanceledException)
        {
            BackpressureMetrics.AddQueueCancellation(transport, operation);
            throw;
        }
        finally
        {
            _ = Interlocked.Decrement(ref _queueDepth);
        }
    }

    private sealed class ClientState
    {
        private int _inFlight;
        private int _queueDepth;

        public ClientState(BackpressureOptions options)
        {
            RateLimiter = RateLimiter.Create(options.PerClientRateLimitPerSecond, options.PerClientRateLimitBurst);
        }

        public int InFlight => Volatile.Read(ref _inFlight);

        public ref int InFlightRef => ref _inFlight;

        public int QueueDepth => Volatile.Read(ref _queueDepth);

        public ref int QueueDepthRef => ref _queueDepth;

        public RateLimiter? RateLimiter { get; }
    }

    private sealed class RateLimiter
    {
        private readonly double _burst;
        private readonly Lock _gate = new();
        private readonly double _ratePerSecond;
        private long _lastTick;
        private double _tokens;

        private RateLimiter(int ratePerSecond, int burst)
        {
            _ratePerSecond = ratePerSecond;
            _burst = burst;
            _tokens = burst;
            _lastTick = Stopwatch.GetTimestamp();
        }

        public bool HasRecentActivity
        {
            get
            {
                lock (_gate)
                {
                    Refill(Stopwatch.GetTimestamp());
                    return _tokens < _burst;
                }
            }
        }

        public static RateLimiter? Create(int? ratePerSecond, int? burst) => ratePerSecond.HasValue && burst.HasValue ? new RateLimiter(ratePerSecond.Value, burst.Value) : null;

        public bool TryAcquire()
        {
            lock (_gate)
            {
                Refill(Stopwatch.GetTimestamp());
                if (_tokens < 1d)
                    return false;

                _tokens -= 1d;
                return true;
            }
        }

        private void Refill(long now)
        {
            var elapsed = Stopwatch.GetElapsedTime(_lastTick, now).TotalSeconds;
            if (elapsed <= 0d)
                return;

            _tokens = Math.Min(_burst, _tokens + (elapsed * _ratePerSecond));
            _lastTick = now;
        }
    }
}
