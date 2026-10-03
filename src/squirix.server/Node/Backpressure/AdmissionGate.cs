using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Backpressure;

internal sealed class AdmissionGate : IBackpressureGate, IDisposable
{
    private readonly ConcurrentDictionary<string, ClientState> _clients = new(StringComparer.Ordinal);
    private readonly BackpressureMetrics _metrics;
    private readonly RateLimiter? _nodeRateLimiter;
    private readonly IDisposable _observerRegistration;
    private readonly AdmissionOptions _options;

    /// <summary>The one client entry all callers share while no per-client limit is set, so admission keeps no per-client state.</summary>
    private readonly ClientState? _sharedClient;
    private readonly AsyncSemaphore _slots;
    private readonly TimeProvider _timeProvider;
    private int _disposed;
    private int _inFlight;
    private int _queueDepth;

    internal AdmissionGate(AdmissionOptions options, BackpressureMetrics metrics, TimeProvider? timeProvider = null)
    {
        _metrics = metrics;
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _options.Validate();
        _slots = new AsyncSemaphore(_options.MaxInFlight);
        _nodeRateLimiter = RateLimiter.Create(_options.NodeRateLimitPerSecond, _options.NodeRateLimitBurst, _timeProvider);
        _sharedClient = _options.HasPerClientLimits ? null : new ClientState(_options, _timeProvider);
        _observerRegistration = _metrics.RegisterObservers(ObserveInFlight, ObserveQueueDepth, ObserveTrackedClients);
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
        var client = _sharedClient ?? _clients.GetOrAdd(clientId, static (_, gate) => new ClientState(gate._options, gate._timeProvider), this);

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
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _observerRegistration.Dispose();
        _slots.Dispose();
    }

    internal void ReleaseLease(string clientId, ClientState client) => Release(clientId, client);

    private async ValueTask<(Decision Decision, Lease Lease)> AcquireFromSlotOrQueueAsync(
        string transport,
        string operation,
        string clientId,
        ClientState client,
        CancellationToken cancellationToken)
    {
        return _slots.TryAcquire() ? (Decision.Accepted(), AcquireLease(clientId, client))
            : await WaitInQueueAsync(transport, operation, clientId, client, cancellationToken).ConfigureAwait(false);
    }

    private Lease AcquireLease(string clientId, ClientState client)
    {
        AdjustInFlight(1);
        _ = Interlocked.Increment(ref client.InFlightRef);
        return new Lease(this, clientId, client);
    }

    private void AdjustInFlight(int adjustment) => _ = Interlocked.Add(ref _inFlight, adjustment);

    private async Task ApplySlowdownAsync(string transport, string operation, int inFlight, CancellationToken cancellationToken)
    {
        var window = Math.Max(1d, _options.RejectThreshold - _options.SlowdownThreshold);
        var relative = Math.Clamp((inFlight - _options.SlowdownThreshold + 1d) / window, 0d, 1d);
        var delay = TimeSpan.FromMilliseconds(_options.MaxSlowdownDelay.TotalMilliseconds * relative);
        if (delay <= TimeSpan.Zero)
            return;

        _metrics.AddSlowdown(transport, operation);
        await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
    }

    private (Decision Decision, Lease Lease)? BypassWhenDisabled(string transport, string operation)
    {
        if (_options.Enabled)
            return null;

        _metrics.AddBypass(transport, operation);
        return (Decision.Accepted(), Lease.Empty);
    }

    private int ObserveInFlight() => Volatile.Read(ref _inFlight);

    private int ObserveQueueDepth() => Volatile.Read(ref _queueDepth);

    private int ObserveTrackedClients() => _clients.Count;

    private (Decision Decision, Lease Lease)? RejectByClientRateLimitIfLimited(string transport, string operation, ClientState client)
    {
        if (!_options.Enabled || client.TryAcquire())
            return null;

        _metrics.AddRateLimitReject(transport, operation, "client");
        _metrics.AddReject(transport, operation, "client_rate_limit");
        return (Decision.Rejected("client_rate_limit"), Lease.Empty);
    }

    private (Decision Decision, Lease Lease)? RejectByHardThresholdIfExceeded(string transport, string operation, int inFlight, int queueDepth)
    {
        if (inFlight < _options.RejectThreshold || queueDepth <= 0)
            return null;

        _metrics.AddReject(transport, operation, "hard_threshold");
        return (Decision.Rejected("hard_threshold"), Lease.Empty);
    }

    private (Decision Decision, Lease Lease)? RejectByNodeRateLimitIfLimited(string transport, string operation)
    {
        if (_nodeRateLimiter?.TryAcquire() != false)
            return null;

        _metrics.AddRateLimitReject(transport, operation, "node");
        _metrics.AddReject(transport, operation, "node_rate_limit");
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
                _metrics.AddReject(transport, operation, "client_queue_full");
                return (Decision.Rejected("client_queue_full"), Lease.Empty);
            }

            _metrics.AddReject(transport, operation, "client_concurrency_limit");
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
        _slots.Release();
        RemoveIdleClient(clientId, client);
    }

    private void RemoveIdleClient(string clientId, ClientState client)
    {
        if (_sharedClient != null || client.InFlight != 0 || client.QueueDepth != 0 || client.HasRecentActivity == true)
            return;

        _ = _clients.TryRemove(new KeyValuePair<string, ClientState>(clientId, client));
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

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
            _metrics.AddReject(transport, operation, "queue_full");
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
                _metrics.AddQueueTimeout(transport, operation);
                _metrics.AddReject(transport, operation, "queue_wait_timeout");
                return (Decision.Rejected("queue_wait_timeout"), Lease.Empty);
            }
            catch (ObjectDisposedException)
            {
                // The gate was disposed while this request was queued, so it never got a slot: reject it like any other
                // refused admission instead of leaking the disposal failure to the caller.
                _metrics.AddReject(transport, operation, "gate_disposed");
                return (Decision.Rejected("gate_disposed"), Lease.Empty);
            }

            var queueWait = Stopwatch.GetElapsedTime(started);
            _metrics.RecordQueueWait(queueWait, transport, operation);
            return (Decision.Accepted(), AcquireLease(clientId, client));
        }
        catch (OperationCanceledException)
        {
            _metrics.AddQueueCancellation(transport, operation);
            throw;
        }
        finally
        {
            _ = Interlocked.Decrement(ref _queueDepth);
        }
    }

    internal sealed class ClientState
    {
        private readonly RateLimiter? _rateLimiter;
        private int _inFlight;
        private int _queueDepth;

        internal ClientState(AdmissionOptions options, TimeProvider timeProvider)
        {
            _rateLimiter = RateLimiter.Create(options.PerClientRateLimitPerSecond, options.PerClientRateLimitBurst, timeProvider);
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
        private readonly TimeProvider _timeProvider;
        private long _lastTick;
        private double _tokens;

        private RateLimiter(int ratePerSecond, int burst, TimeProvider timeProvider)
        {
            _ratePerSecond = ratePerSecond;
            _burst = burst;
            _tokens = burst;
            _timeProvider = timeProvider;
            _lastTick = timeProvider.GetTimestamp();
        }

        internal bool HasRecentActivity
        {
            get
            {
                lock (_gate)
                {
                    Refill(_timeProvider.GetTimestamp());
                    return _tokens < _burst;
                }
            }
        }

        internal static RateLimiter? Create(int? ratePerSecond, int? burst, TimeProvider timeProvider) =>
            ratePerSecond != null && burst != null ? new RateLimiter(ratePerSecond.Value, burst.Value, timeProvider) : null;

        internal bool TryAcquire()
        {
            lock (_gate)
            {
                Refill(_timeProvider.GetTimestamp());
                if (_tokens < 1d)
                    return false;

                _tokens--;
                return true;
            }
        }

        private void Refill(long now)
        {
            var elapsed = _timeProvider.GetElapsedTime(_lastTick, now).TotalSeconds;
            if (elapsed <= 0d)
                return;

            _tokens = Math.Min(_burst, _tokens + (elapsed * _ratePerSecond));
            _lastTick = now;
        }
    }
}
