using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Observability;

[Mutable]
internal sealed class BackpressureMetrics
{
    private static readonly ILogger Logger = LogManager.GetLogger("Squirix.Server.Node.Observability.BackpressureMetrics");
    private readonly Counter<long> _bypassTotalCtr;

    private readonly Meter _meter;
    private readonly Lock _observerGate = new();
    private readonly Dictionary<long, ObserverEntry> _observers = [];
    private readonly ObserverState _observersState = new();
    private readonly Counter<long> _queueCancellationsTotalCtr;
    private readonly Counter<long> _queueTimeoutsTotalCtr;
    private readonly Histogram<double> _queueWaitHist;
    private readonly Counter<long> _rateLimitRejectTotalCtr;
    private readonly Counter<long> _rejectTotalCtr;
    private readonly Counter<long> _slowdownTotalCtr;

    internal BackpressureMetrics(Meter meter)
    {
        _meter = meter;
        _bypassTotalCtr = meter.CreateCounter<long>("squirix_backpressure_bypass_total");
        _queueCancellationsTotalCtr = meter.CreateCounter<long>("squirix_backpressure_queue_cancellations_total");
        _queueTimeoutsTotalCtr = meter.CreateCounter<long>("squirix_backpressure_queue_timeouts_total");
        _queueWaitHist = meter.CreateHistogram<double>("squirix_backpressure_queue_wait_seconds");
        _rateLimitRejectTotalCtr = meter.CreateCounter<long>("squirix_backpressure_rate_limit_reject_total");
        _rejectTotalCtr = meter.CreateCounter<long>("squirix_backpressure_reject_total");
        _slowdownTotalCtr = meter.CreateCounter<long>("squirix_backpressure_slowdown_total");
    }

    internal void AddBypass(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        _bypassTotalCtr.Add(1, in tags);
    }

    internal void AddQueueCancellation(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        _queueCancellationsTotalCtr.Add(1, in tags);
    }

    internal void AddQueueTimeout(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        _queueTimeoutsTotalCtr.Add(1, in tags);
    }

    internal void AddRateLimitReject(string transport, string operation, string scope)
    {
        var tags = CreateTags(transport, operation, ("scope", scope));
        _rateLimitRejectTotalCtr.Add(1, in tags);
    }

    internal void AddReject(string transport, string operation, string reason)
    {
        var tags = CreateTags(transport, operation, ("reason", reason));
        _rejectTotalCtr.Add(1, in tags);
    }

    internal void AddSlowdown(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        _slowdownTotalCtr.Add(1, in tags);
    }

    internal void RecordQueueWait(TimeSpan duration, string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        _queueWaitHist.Record(duration.TotalSeconds, in tags);
    }

    internal IDisposable RegisterObservers(Func<int> observeInFlight, Func<int> observeQueueDepth, Func<int> observeTrackedClients)
    {
        ArgumentNullException.ThrowIfNull(observeInFlight);
        ArgumentNullException.ThrowIfNull(observeQueueDepth);
        ArgumentNullException.ThrowIfNull(observeTrackedClients);

        var observerId = _observersState.AllocateObserverId();
        var entry = new ObserverEntry(observeInFlight, observeQueueDepth, observeTrackedClients);
        lock (_observerGate)
            _observers[observerId] = entry;

        if (!_observersState.TryRegisterObservers())
            return new ObserverRegistration(observerId, this);

        _ = _meter.CreateObservableGauge(
            "squirix_backpressure_in_flight",
            () => new Measurement<int>(Aggregate(static e => e.ObserveInFlight())),
            description: "Current number of admitted in-flight requests");
        _ = _meter.CreateObservableGauge(
            "squirix_backpressure_queue_depth",
            () => new Measurement<int>(Aggregate(static e => e.ObserveQueueDepth())),
            description: "Current number of requests waiting for admission");
        _ = _meter.CreateObservableGauge(
            "squirix_backpressure_tracked_clients",
            () => new Measurement<int>(Aggregate(static e => e.ObserveTrackedClients())),
            description: "Current number of client buckets tracked for backpressure state");

        return new ObserverRegistration(observerId, this);
    }

    private static TagList CreateTags(string transport, string operation, (string Key, string Value)? extra = null)
    {
        var tags = new TagList
        {
            { "transport", transport },
            { "op", operation },
        };

        if (extra is { } pair)
            tags.Add(pair.Key, pair.Value);

        return tags;
    }

    private int Aggregate(Func<ObserverEntry, int> selector)
    {
        lock (_observerGate)
        {
            var total = 0;
            foreach (var entry in _observers.Values)
            {
                try
                {
                    total += selector(entry);
                }
                catch (ObjectDisposedException ex)
                {
                    // Keep metrics observation resilient if one observer source is torn down concurrently.
                    LogManager.BackpressureObservationFailed(Logger, ex);
                }
                catch (InvalidOperationException ex)
                {
                    // Keep metrics observation resilient if one observer source is torn down concurrently.
                    LogManager.BackpressureObservationFailed(Logger, ex);
                }
            }

            return total;
        }
    }

    [Immutable]
    private sealed class ObserverEntry
    {
        internal ObserverEntry(Func<int> observeInFlight, Func<int> observeQueueDepth, Func<int> observeTrackedClients)
        {
            ObserveInFlight = observeInFlight;
            ObserveQueueDepth = observeQueueDepth;
            ObserveTrackedClients = observeTrackedClients;
        }

        internal Func<int> ObserveInFlight { get; }

        internal Func<int> ObserveQueueDepth { get; }

        internal Func<int> ObserveTrackedClients { get; }
    }

    [Immutable]
    private sealed class ObserverRegistration : IDisposable
    {
        private readonly long _observerId;
        private readonly BackpressureMetrics _owner;
        private int _disposed;

        internal ObserverRegistration(long observerId, BackpressureMetrics owner)
        {
            _observerId = observerId;
            _owner = owner;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            lock (_owner._observerGate)
                _ = _owner._observers.Remove(_observerId);
        }
    }

    [Immutable]
    private sealed class ObserverState
    {
        private readonly MutableInt64 _nextObserverId = new();

        private readonly MutableInt32 _registered = new();

        internal long AllocateObserverId() => Interlocked.Increment(ref _nextObserverId.Value);

        internal bool TryRegisterObservers() => Interlocked.Exchange(ref _registered.Value, 1) == 0;
    }
}
