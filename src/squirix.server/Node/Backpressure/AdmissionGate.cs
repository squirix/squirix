using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Backpressure;

internal sealed class AdmissionGate : IBackpressureGate, IDisposable
{
    private readonly ConcurrentDictionary<string, ClientState> _clients = new(StringComparer.Ordinal);
    private readonly RateLimiter? _nodeRateLimiter;
    private readonly IDisposable _observerRegistration;
    private readonly AdmissionOptions _options;
    private readonly SemaphoreSlim _slots;
    private readonly TimeProvider _timeProvider;
    private bool _disposed;
    private int _inFlight;
    private int _queueDepth;

    internal AdmissionGate(AdmissionOptions options, TimeProvider? timeProvider = null)
    {
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _options.Validate();
        _slots = new SemaphoreSlim(_options.MaxInFlight, _options.MaxInFlight);
        _nodeRateLimiter = RateLimiter.Create(_options.NodeRateLimitPerSecond, _options.NodeRateLimitBurst);
        _observerRegistration = BackpressureMetrics.RegisterObservers(ObserveInFlight, ObserveQueueDepth, ObserveTrackedClients);
    }

    public async ValueTask<(Decision Decision, Lease Lease)> AcquireAsync(string transport, string operation, string clientId, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(transport);
        ArgumentException.ThrowIfNullOrWhiteSpace(operation);
        ArgumentException.ThrowIfNullOrWhiteSpace(clientId);

        var disabledResult = BypassWhenDisabled(transport, operation);
        if (disabledResult != null)
            return disabledResult.Value;

        cancellationToken.ThrowIfCancellationRequested();
        var client = _clients.GetOrAdd(clientId, static (_, options) => new ClientState(options), _options);

        var nodeRateLimitReject = RejectByNodeRateLimitIfLimited(transport, operation);
        if (nodeRateLimitReject != null)
            return nodeRateLimitReject.Value;

        var clientRateLimitReject = RejectByClientRateLimitIfLimited(transport, operation, client);
        if (clientRateLimitReject != null)
            return clientRateLimitReject.Value;

        var inFlight = Volatile.Read(ref _inFlight);
        var queueDepth = Volatile.Read(ref _queueDepth);
        var hardThresholdReject = RejectByHardThresholdIfExceeded(transport, operation, inFlight, queueDepth);
        if (hardThresholdReject != null)
            return hardThresholdReject.Value;

        if (inFlight >= _options.SlowdownThreshold)
            await ApplySlowdownAsync(transport, operation, inFlight, cancellationToken).ConfigureAwait(false);

        var clientConcurrencyReject = RejectByPerClientConcurrencyIfLimited(transport, operation, clientId, client);
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

    internal void ReleaseLease(string clientId)
    {
        if (_clients.TryGetValue(clientId, out var client))
        {
            Release(clientId, client);
            return;
        }

        AdjustInFlight(-1);
        _ = _slots.Release();
    }

    private async ValueTask<(Decision Decision, Lease Lease)> AcquireFromSlotOrQueueAsync(
        string transport,
        string operation,
        string clientId,
        ClientState client,
        CancellationToken cancellationToken)
    {
        return await _slots.WaitAsync(0, cancellationToken).ConfigureAwait(false) ? (Decision.Accepted(), AcquireLease(clientId, client))
            : await WaitInQueueAsync(transport, operation, clientId, client, cancellationToken).ConfigureAwait(false);
    }

    private Lease AcquireLease(string clientId, ClientState client)
    {
        AdjustInFlight(1);
        _ = Interlocked.Increment(ref client.InFlightRef);
        return new Lease(this, clientId);
    }

    private void AdjustInFlight(int adjustment) => _ = Interlocked.Add(ref _inFlight, adjustment);

    private async Task ApplySlowdownAsync(string transport, string operation, int inFlight, CancellationToken cancellationToken)
    {
        var window = Math.Max(1d, _options.RejectThreshold - _options.SlowdownThreshold);
        var relative = Math.Clamp((inFlight - _options.SlowdownThreshold + 1d) / window, 0d, 1d);
        var delay = TimeSpan.FromMilliseconds(_options.MaxSlowdownDelay.TotalMilliseconds * relative);
        if (delay <= TimeSpan.Zero)
            return;

        BackpressureMetrics.AddSlowdown(transport, operation);
        await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
    }

    private (Decision Decision, Lease Lease)? BypassWhenDisabled(string transport, string operation)
    {
        if (_options.Enabled)
            return null;

        BackpressureMetrics.AddBypass(transport, operation);
        return (Decision.Accepted(), Lease.Empty);
    }

    private int ObserveInFlight() => Volatile.Read(ref _inFlight);

    private int ObserveQueueDepth() => Volatile.Read(ref _queueDepth);

    private int ObserveTrackedClients() => _clients.Count;

    private (Decision Decision, Lease Lease)? RejectByClientRateLimitIfLimited(string transport, string operation, ClientState client)
    {
        if (!_options.Enabled || client.TryAcquire())
            return null;

        BackpressureMetrics.AddRateLimitReject(transport, operation, "client");
        BackpressureMetrics.AddReject(transport, operation, "client_rate_limit");
        return (Decision.Rejected("client_rate_limit"), Lease.Empty);
    }

    private (Decision Decision, Lease Lease)? RejectByHardThresholdIfExceeded(string transport, string operation, int inFlight, int queueDepth)
    {
        if (inFlight < _options.RejectThreshold || queueDepth <= 0)
            return null;

        BackpressureMetrics.AddReject(transport, operation, "hard_threshold");
        return (Decision.Rejected("hard_threshold"), Lease.Empty);
    }

    private (Decision Decision, Lease Lease)? RejectByNodeRateLimitIfLimited(string transport, string operation)
    {
        if (_nodeRateLimiter?.TryAcquire() != false)
            return null;

        BackpressureMetrics.AddRateLimitReject(transport, operation, "node");
        BackpressureMetrics.AddReject(transport, operation, "node_rate_limit");
        return (Decision.Rejected("node_rate_limit"), Lease.Empty);
    }

    private (Decision Decision, Lease Lease)? RejectByPerClientConcurrencyIfLimited(string transport, string operation, string clientId, ClientState client)
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
                return (Decision.Rejected("client_queue_full"), Lease.Empty);
            }

            BackpressureMetrics.AddReject(transport, operation, "client_concurrency_limit");
            return (Decision.Rejected("client_concurrency_limit"), Lease.Empty);
        }
        finally
        {
            _ = Interlocked.Decrement(ref client.QueueDepthRef);
            RemoveIdleClient(clientId, client);
        }
    }

    private void Release(string clientId, ClientState client)
    {
        _ = Interlocked.Decrement(ref client.InFlightRef);
        AdjustInFlight(-1);
        _ = _slots.Release();
        RemoveIdleClient(clientId, client);
    }

    private void RemoveIdleClient(string clientId, ClientState client)
    {
        if (client.InFlight != 0 || client.QueueDepth != 0 || client.HasRecentActivity == true)
            return;

        _ = _clients.TryRemove(new KeyValuePair<string, ClientState>(clientId, client));
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(_disposed, this);

    private async ValueTask<(Decision Decision, Lease Lease)> WaitInQueueAsync(
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
            return (Decision.Rejected("queue_full"), Lease.Empty);
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
                return (Decision.Rejected("queue_wait_timeout"), Lease.Empty);
            }

            var queueWait = Stopwatch.GetElapsedTime(started);
            BackpressureMetrics.RecordQueueWait(queueWait, transport, operation);
            return (Decision.Accepted(), AcquireLease(clientId, client));
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
        private readonly RateLimiter? _rateLimiter;
        private int _inFlight;
        private int _queueDepth;

        internal ClientState(AdmissionOptions options)
        {
            _rateLimiter = RateLimiter.Create(options.PerClientRateLimitPerSecond, options.PerClientRateLimitBurst);
        }

        internal bool? HasRecentActivity => _rateLimiter?.HasRecentActivity;

        internal int InFlight => Volatile.Read(ref _inFlight);

        internal ref int InFlightRef => ref _inFlight;

        internal int QueueDepth => Volatile.Read(ref _queueDepth);

        internal ref int QueueDepthRef => ref _queueDepth;

        internal bool TryAcquire() => _rateLimiter?.TryAcquire() != false;
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

        internal bool HasRecentActivity
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

        internal static RateLimiter? Create(int? ratePerSecond, int? burst) => ratePerSecond != null && burst != null ? new RateLimiter(ratePerSecond.Value, burst.Value) : null;

        internal bool TryAcquire()
        {
            lock (_gate)
            {
                Refill(Stopwatch.GetTimestamp());
                if (_tokens < 1d)
                    return false;

                _tokens--;
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
