using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;

namespace Squirix.Server.Node.Observability;

internal static class BackpressureMetrics
{
    private static readonly Counter<long> BypassTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_bypass_total");

    private static readonly Lock ObserverGate = new();
    private static readonly Dictionary<long, ObserverEntry> Observers = [];
    private static readonly Counter<long> QueueCancellationsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_queue_cancellations_total");
    private static readonly Counter<long> QueueTimeoutsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_queue_timeouts_total");
    private static readonly Histogram<double> QueueWaitHist = MeterRegistry.Meter.CreateHistogram<double>("squirix_backpressure_queue_wait_seconds");
    private static readonly Counter<long> RateLimitRejectTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_rate_limit_reject_total");
    private static readonly Counter<long> RejectTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_reject_total");
    private static readonly Counter<long> SlowdownTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_slowdown_total");
    private static long _nextObserverId;
    private static int _registered;

    public static void AddBypass(string transport, string operation) => BypassTotalCtr.Add(1, CreateTags(transport, operation));

    public static void AddQueueCancellation(string transport, string operation) => QueueCancellationsTotalCtr.Add(1, CreateTags(transport, operation));

    public static void AddQueueTimeout(string transport, string operation) => QueueTimeoutsTotalCtr.Add(1, CreateTags(transport, operation));

    public static void AddRateLimitReject(string transport, string operation, string scope) => RateLimitRejectTotalCtr.Add(1, CreateTags(transport, operation, ("scope", scope)));

    public static void AddReject(string transport, string operation, string reason) => RejectTotalCtr.Add(1, CreateTags(transport, operation, ("reason", reason)));

    public static void AddSlowdown(string transport, string operation) => SlowdownTotalCtr.Add(1, CreateTags(transport, operation));

    public static void RecordQueueWait(TimeSpan duration, string transport, string operation) => QueueWaitHist.Record(duration.TotalSeconds, CreateTags(transport, operation));

    public static IDisposable RegisterObservers(Func<int> observeInFlight, Func<int> observeQueueDepth, Func<int> observeTrackedClients)
    {
        ArgumentNullException.ThrowIfNull(observeInFlight);
        ArgumentNullException.ThrowIfNull(observeQueueDepth);
        ArgumentNullException.ThrowIfNull(observeTrackedClients);

        var observerId = Interlocked.Increment(ref _nextObserverId);
        var entry = new ObserverEntry(observeInFlight, observeQueueDepth, observeTrackedClients);
        lock (ObserverGate)
        {
            Observers[observerId] = entry;
        }

        if (Interlocked.Exchange(ref _registered, 1) != 0)
            return new ObserverRegistration(observerId);

        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_backpressure_in_flight",
            static () => new[] { new Measurement<int>(Aggregate(static e => e.ObserveInFlight())) },
            description: "Current number of admitted in-flight requests");
        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_backpressure_queue_depth",
            static () => new[] { new Measurement<int>(Aggregate(static e => e.ObserveQueueDepth())) },
            description: "Current number of requests waiting for admission");
        _ = MeterRegistry.Meter.CreateObservableGauge(
            "squirix_backpressure_tracked_clients",
            static () => new[] { new Measurement<int>(Aggregate(static e => e.ObserveTrackedClients())) },
            description: "Current number of client buckets tracked for backpressure state");

        return new ObserverRegistration(observerId);
    }

    private static int Aggregate(Func<ObserverEntry, int> selector)
    {
        ObserverEntry[] snapshot;
        lock (ObserverGate)
        {
            snapshot = [.. Observers.Values];
        }

        var total = 0;
        foreach (var observer in snapshot)
        {
            try
            {
                total += selector(observer);
            }
            catch (ObjectDisposedException)
            {
                // Keep metrics observation resilient if one observer source is torn down concurrently.
            }
            catch (InvalidOperationException)
            {
                // Keep metrics observation resilient if one observer source is torn down concurrently.
            }
        }

        return total;
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

    private sealed class ObserverEntry
    {
        public ObserverEntry(Func<int> observeInFlight, Func<int> observeQueueDepth, Func<int> observeTrackedClients)
        {
            ObserveInFlight = observeInFlight;
            ObserveQueueDepth = observeQueueDepth;
            ObserveTrackedClients = observeTrackedClients;
        }

        public Func<int> ObserveInFlight { get; }

        public Func<int> ObserveQueueDepth { get; }

        public Func<int> ObserveTrackedClients { get; }
    }

    private sealed class ObserverRegistration : IDisposable
    {
        private readonly long _observerId;
        private int _disposed;

        public ObserverRegistration(long observerId)
        {
            _observerId = observerId;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            lock (ObserverGate)
            {
                _ = Observers.Remove(_observerId);
            }
        }
    }
}
