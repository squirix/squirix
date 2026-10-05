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
    /// <summary>The most entries one sweep examines, so the cost an admission pays for it stays bounded.</summary>
    private const int SweepBudget = 256;

    private readonly ConcurrentDictionary<string, ClientState> _clients = new(StringComparer.Ordinal);
    private readonly BackpressureMetrics _metrics;
    private readonly RateLimiter? _nodeRateLimiter;
    private readonly IDisposable _observerRegistration;
    private readonly AdmissionOptions _options;

    /// <summary>The one client entry all callers share while no per-client limit is set, so admission keeps no per-client state.</summary>
    private readonly ClientState? _sharedClient;
    private readonly AsyncSemaphore _slots;
    private readonly TimeProvider _timeProvider;

    /// <summary>The client entry of internal owner-routed calls: it carries no per-client limit, so only node-wide limits apply to them.</summary>
    private readonly ClientState _unmeteredClient = new();
    private int _disposed;
    private int _inFlight;
    private long _nextSweepTimestamp;
    private int _queueDepth;
    private int _sweepCursor;

    internal AdmissionGate(AdmissionOptions options, BackpressureMetrics metrics, TimeProvider? timeProvider = null)
    {
        _metrics = metrics;
        _options = options;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _options.Validate();

        // Registered before the semaphore is created, so a refused registration leaves nothing to dispose.
        _observerRegistration = _metrics.RegisterObservers(() => InFlight, () => QueueDepth, () => TrackedClients);
        _slots = new AsyncSemaphore(_options.MaxInFlight);
        _nodeRateLimiter = RateLimiter.Create(_options.NodeRateLimitPerSecond, _options.NodeRateLimitBurst, _timeProvider);
        _sharedClient = _options.HasPerClientLimits ? null : new ClientState(_options, _timeProvider, false);
        _nextSweepTimestamp = _timeProvider.GetTimestamp() + _timeProvider.TimestampFrequency;
    }

    /// <summary>Gets the number of admitted requests holding a slot.</summary>
    internal int InFlight => Volatile.Read(ref _inFlight);

    /// <summary>Gets the number of requests waiting in the node queue.</summary>
    internal int QueueDepth => Volatile.Read(ref _queueDepth);

    /// <summary>Gets the number of per-client entries the gate currently tracks.</summary>
    internal int TrackedClients => _clients.Count;

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
        SweepIdleClientsIfDue();
        var client = ResolveClient(clientId);

        // The client entry is created above, so every exit that does not hand it to the slot or queue step gives it back if it is idle.
        var handedOver = false;
        try
        {
            var nodeRateLimitReject = RejectByNodeRateLimitIfLimited(transport, operation);
            if (nodeRateLimitReject != null)
                return nodeRateLimitReject.Value;

            var clientRateLimitReject = RejectByClientRateLimitIfLimited(transport, operation, client);
            if (clientRateLimitReject != null)
                return clientRateLimitReject.Value;

            var inFlight = Volatile.Read(ref _inFlight);
            var queueDepth = Volatile.Read(ref _queueDepth);
            var queueFullReject = RejectByQueueFullIfSaturated(transport, operation, inFlight, queueDepth);
            if (queueFullReject != null)
                return queueFullReject.Value;

            if (inFlight >= _options.SlowdownThreshold)
                await ApplySlowdownAsync(transport, operation, inFlight, cancellationToken).ConfigureAwait(false);

            var clientConcurrencyReject = RejectByPerClientConcurrencyIfLimited(transport, operation, clientId, ref client);
            if (clientConcurrencyReject != null)
                return clientConcurrencyReject.Value;

            handedOver = true;
            return await AcquireFromSlotOrQueueAsync(transport, operation, clientId, client, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (!handedOver)
                RemoveIdleClient(clientId, client);
        }
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
        // The per-client reservation taken before this step is handed over to the lease on admission and undone on every other exit.
        var admitted = false;
        try
        {
            if (_slots.TryAcquire())
            {
                admitted = true;
                return (Decision.Accepted(), AcquireLease(clientId, client));
            }

            var queued = await WaitInQueueAsync(transport, operation, clientId, client, cancellationToken).ConfigureAwait(false);
            admitted = queued.Decision.IsAccepted;
            return queued;
        }
        finally
        {
            if (!admitted && client.IsPerClient)
            {
                _ = Interlocked.Decrement(ref client.InFlightRef);
                RemoveIdleClient(clientId, client);
            }
        }
    }

    private Lease AcquireLease(string clientId, ClientState client)
    {
        AdjustInFlight(1);
        return new Lease(this, clientId, client);
    }

    private void AdjustInFlight(int adjustment) => _ = Interlocked.Add(ref _inFlight, adjustment);

    private async Task ApplySlowdownAsync(string transport, string operation, int inFlight, CancellationToken cancellationToken)
    {
        var window = Math.Max(1d, _options.MaxInFlight - _options.SlowdownThreshold);
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

    private (Decision Decision, Lease Lease)? RejectByClientRateLimitIfLimited(string transport, string operation, ClientState client)
    {
        if (client.TryAcquire())
            return null;

        _metrics.AddRateLimitReject(transport, operation, "client");
        _metrics.AddReject(transport, operation, "client_rate_limit");
        return (Decision.Rejected("client_rate_limit"), Lease.Empty);
    }

    private (Decision Decision, Lease Lease)? RejectByNodeRateLimitIfLimited(string transport, string operation)
    {
        if (_nodeRateLimiter?.TryAcquire() != false)
            return null;

        _metrics.AddRateLimitReject(transport, operation, "node");
        _metrics.AddReject(transport, operation, "node_rate_limit");
        return (Decision.Rejected("node_rate_limit"), Lease.Empty);
    }

    private (Decision Decision, Lease Lease)? RejectByPerClientConcurrencyIfLimited(string transport, string operation, string clientId, ref ClientState client)
    {
        if (!client.IsPerClient)
            return null;

        // Admitted and queued requests of one client share this counter, so a client cannot exceed its limit by queueing.
        // An entry retired after this request resolved it is dead: reserving on it would orphan the count, so it is replaced by the live entry.
        // Only the concurrency limit is strict across a retire; the per-client rate limit is soft, because the request may have taken a token on the old entry.
        var reserved = Interlocked.Increment(ref client.InFlightRef);
        while (reserved < 0)
        {
            _ = _clients.TryRemove(new KeyValuePair<string, ClientState>(clientId, client));
            client = ResolveClient(clientId);
            reserved = Interlocked.Increment(ref client.InFlightRef);
        }

        if (_options.PerClientMaxInFlight is not { } perClientMaxInFlight || reserved <= perClientMaxInFlight)
            return null;

        _ = Interlocked.Decrement(ref client.InFlightRef);
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
        }
    }

    private (Decision Decision, Lease Lease)? RejectByQueueFullIfSaturated(string transport, string operation, int inFlight, int queueDepth)
    {
        // A snapshot shortcut that spares a doomed request the slowdown delay; the queue step stays the authoritative check.
        if (inFlight < _options.MaxInFlight || queueDepth < _options.MaxQueue)
            return null;

        _metrics.AddReject(transport, operation, "queue_full");
        return (Decision.Rejected("queue_full"), Lease.Empty);
    }

    private void Release(string clientId, ClientState client)
    {
        if (client.IsPerClient)
            _ = Interlocked.Decrement(ref client.InFlightRef);

        AdjustInFlight(-1);
        _slots.Release();
        RemoveIdleClient(clientId, client);
    }

    private void RemoveIdleClient(string clientId, ClientState client)
    {
        if (_sharedClient != null || !client.IsPerClient || client.QueueDepth != 0 || client.HasRecentActivity == true || !client.TryRetire())
            return;

        _ = _clients.TryRemove(new KeyValuePair<string, ClientState>(clientId, client));
    }

    /// <summary>
    /// Evicts idle client entries a bounded batch at a time, at most once per sweep interval, driven by admissions.
    /// An entry goes only when nothing is in flight or queued for it and its rate-limit bucket would already be full again,
    /// so its replacement starting with a full burst changes no admission decision. Retiring uses the same protocol as the
    /// last release of a request, so the per-client concurrency limit stays strict.
    /// </summary>
    private void SweepIdleClientsIfDue()
    {
        if (_sharedClient != null)
            return;

        var due = Volatile.Read(ref _nextSweepTimestamp);
        var now = _timeProvider.GetTimestamp();
        if (now < due)
            return;

        // One caller wins the claim and sweeps; the others carry on with their admission.
        // The sweep interval is one second of the gate time provider, which is exactly one timestamp frequency.
        var next = now + _timeProvider.TimestampFrequency;
        if (Interlocked.CompareExchange(ref _nextSweepTimestamp, next, due) != due)
            return;

        var skip = Volatile.Read(ref _sweepCursor);
        var position = 0;
        var examined = 0;
        foreach (var entry in _clients)
        {
            if (position++ < skip)
                continue;

            if (examined++ >= SweepBudget)
            {
                Volatile.Write(ref _sweepCursor, position - 1);
                return;
            }

            var client = entry.Value;
            if (client.QueueDepth == 0 && client.IsRefilled(now) && client.TryRetire())
            {
                _ = _clients.TryRemove(new KeyValuePair<string, ClientState>(entry.Key, client));
                position--;
            }
        }

        Volatile.Write(ref _sweepCursor, 0);
    }

    private ClientState ResolveClient(string clientId) => _sharedClient ?? (string.Equals(clientId, HttpContextClientIdResolver.InternalOwnerClientId, StringComparison.Ordinal)
        ? _unmeteredClient
        : _clients.GetOrAdd(clientId, static (_, gate) => new ClientState(gate._options, gate._timeProvider), this));

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

        internal ClientState()
        {
        }

        internal ClientState(AdmissionOptions options, TimeProvider timeProvider, bool isPerClient = true)
        {
            IsPerClient = isPerClient;
            _rateLimiter = RateLimiter.Create(options.PerClientRateLimitPerSecond, options.PerClientRateLimitBurst, timeProvider);
        }

        /// <summary>Gets a value indicating whether this entry belongs to one client and counts its in-flight requests; shared and unmetered entries do not.</summary>
        internal bool IsPerClient { get; }

        internal bool? HasRecentActivity => _rateLimiter?.HasRecentActivity;

        internal ref int InFlightRef => ref _inFlight;

        internal int QueueDepth => Volatile.Read(ref _queueDepth);

        internal ref int QueueDepthRef => ref _queueDepth;

        /// <summary>Marks an entry with no admitted or waiting request as dead, so no later reservation can use it.</summary>
        /// <returns><see langword="true" /> when the entry was retired.</returns>
        internal bool TryRetire() => Interlocked.CompareExchange(ref _inFlight, int.MinValue, 0) == 0;

        /// <summary>Checks, without changing the bucket, whether it would be full at <paramref name="now" />.</summary>
        /// <param name="now">The timestamp of the gate time provider to evaluate at.</param>
        /// <returns><see langword="true" /> when there is no bucket or it has refilled to its burst.</returns>
        internal bool IsRefilled(long now) => _rateLimiter?.IsRefilled(now) != false;

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

        internal bool IsRefilled(long now)
        {
            lock (_gate)
            {
                var elapsed = Math.Max(0d, _timeProvider.GetElapsedTime(_lastTick, now).TotalSeconds);
                return _tokens + (elapsed * _ratePerSecond) >= _burst;
            }
        }

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
