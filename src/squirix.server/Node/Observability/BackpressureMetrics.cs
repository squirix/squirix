using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

[Mutable]
internal sealed class BackpressureMetrics
{
    private readonly Counter<long> _bypassTotalCtr;
    private readonly Counter<long> _queueCancellationsTotalCtr;
    private readonly Counter<long> _queueTimeoutsTotalCtr;
    private readonly Histogram<double> _queueWaitHist;
    private readonly Counter<long> _rateLimitRejectTotalCtr;
    private readonly Counter<long> _rejectTotalCtr;
    private readonly Counter<long> _slowdownTotalCtr;

    /// <summary>The admission gate the gauges read, or <see langword="null" /> while none is registered.</summary>
    private Observer? _observer;

    internal BackpressureMetrics(Meter meter)
    {
        ArgumentNullException.ThrowIfNull(meter);
        _bypassTotalCtr = meter.CreateCounter<long>("squirix_backpressure_bypass_total");
        _queueCancellationsTotalCtr = meter.CreateCounter<long>("squirix_backpressure_queue_cancellations_total");
        _queueTimeoutsTotalCtr = meter.CreateCounter<long>("squirix_backpressure_queue_timeouts_total");
        _queueWaitHist = meter.CreateHistogram<double>("squirix_backpressure_queue_wait_seconds");
        _rateLimitRejectTotalCtr = meter.CreateCounter<long>("squirix_backpressure_rate_limit_reject_total");
        _rejectTotalCtr = meter.CreateCounter<long>("squirix_backpressure_reject_total");
        _slowdownTotalCtr = meter.CreateCounter<long>("squirix_backpressure_slowdown_total");
        _ = meter.CreateObservableGauge(
            "squirix_backpressure_in_flight",
            () => new Measurement<int>(Volatile.Read(ref _observer)?.InFlight() ?? 0),
            description: "Current number of admitted in-flight requests");
        _ = meter.CreateObservableGauge(
            "squirix_backpressure_queue_depth",
            () => new Measurement<int>(Volatile.Read(ref _observer)?.QueueDepth() ?? 0),
            description: "Current number of requests waiting for admission");
        _ = meter.CreateObservableGauge(
            "squirix_backpressure_tracked_clients",
            () => new Measurement<int>(Volatile.Read(ref _observer)?.TrackedClients() ?? 0),
            description: "Current number of client buckets tracked for backpressure state; zero unless a per-client limit is set");
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

    /// <summary>Makes the gauges read the given admission gate counters until the returned registration is disposed.</summary>
    /// <param name="observeInFlight">Reads the admitted in-flight request count.</param>
    /// <param name="observeQueueDepth">Reads the queued request count.</param>
    /// <param name="observeTrackedClients">Reads the tracked client bucket count.</param>
    /// <returns>The registration that stops the gauges reading the gate.</returns>
    /// <exception cref="InvalidOperationException">Thrown when another gate is already registered.</exception>
    internal IDisposable RegisterObservers(Func<int> observeInFlight, Func<int> observeQueueDepth, Func<int> observeTrackedClients)
    {
        ArgumentNullException.ThrowIfNull(observeInFlight);
        ArgumentNullException.ThrowIfNull(observeQueueDepth);
        ArgumentNullException.ThrowIfNull(observeTrackedClients);

        var observer = new Observer(this, observeInFlight, observeQueueDepth, observeTrackedClients);
        return Interlocked.CompareExchange(ref _observer, observer, null) == null
            ? observer
            : throw new InvalidOperationException("Backpressure metrics already observe an admission gate.");
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

    /// <summary>The counters of one admission gate; disposing it stops the gauges reading them.</summary>
    [Immutable]
    private sealed class Observer : IDisposable
    {
        private readonly BackpressureMetrics _owner;

        internal Observer(BackpressureMetrics owner, Func<int> inFlight, Func<int> queueDepth, Func<int> trackedClients)
        {
            _owner = owner;
            InFlight = inFlight;
            QueueDepth = queueDepth;
            TrackedClients = trackedClients;
        }

        internal Func<int> InFlight { get; }

        internal Func<int> QueueDepth { get; }

        internal Func<int> TrackedClients { get; }

        public void Dispose() => _ = Interlocked.CompareExchange(ref _owner._observer, null, this);
    }
}
