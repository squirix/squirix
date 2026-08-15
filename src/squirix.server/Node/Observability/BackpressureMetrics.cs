using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Observability;

internal static class BackpressureMetrics
{
    private static readonly Counter<long> BypassTotalCtr = ServerMeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_bypass_total");

    private static readonly Lock ObserverGate = new();
    private static readonly Dictionary<long, ObserverEntry> Observers = [];
    private static readonly ObserverState ObserversState = new();
    private static readonly Counter<long> QueueCancellationsTotalCtr = ServerMeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_queue_cancellations_total");
    private static readonly Counter<long> QueueTimeoutsTotalCtr = ServerMeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_queue_timeouts_total");
    private static readonly Histogram<double> QueueWaitHist = ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_backpressure_queue_wait_seconds");
    private static readonly Counter<long> RateLimitRejectTotalCtr = ServerMeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_rate_limit_reject_total");
    private static readonly Counter<long> RejectTotalCtr = ServerMeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_reject_total");
    private static readonly Counter<long> SlowdownTotalCtr = ServerMeterRegistry.Meter.CreateCounter<long>("squirix_backpressure_slowdown_total");
    private static ObserverEntry[] _snapshotBuffer = [];

    internal static void AddBypass(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        BypassTotalCtr.Add(1, in tags);
    }

    internal static void AddQueueCancellation(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        QueueCancellationsTotalCtr.Add(1, in tags);
    }

    internal static void AddQueueTimeout(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        QueueTimeoutsTotalCtr.Add(1, in tags);
    }

    internal static void AddRateLimitReject(string transport, string operation, string scope)
    {
        var tags = CreateTags(transport, operation, ("scope", scope));
        RateLimitRejectTotalCtr.Add(1, in tags);
    }

    internal static void AddReject(string transport, string operation, string reason)
    {
        var tags = CreateTags(transport, operation, ("reason", reason));
        RejectTotalCtr.Add(1, in tags);
    }

    internal static void AddSlowdown(string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        SlowdownTotalCtr.Add(1, in tags);
    }

    internal static void RecordQueueWait(TimeSpan duration, string transport, string operation)
    {
        var tags = CreateTags(transport, operation);
        QueueWaitHist.Record(duration.TotalSeconds, in tags);
    }

    internal static IDisposable RegisterObservers(Func<int> observeInFlight, Func<int> observeQueueDepth, Func<int> observeTrackedClients)
    {
        ArgumentNullException.ThrowIfNull(observeInFlight);
        ArgumentNullException.ThrowIfNull(observeQueueDepth);
        ArgumentNullException.ThrowIfNull(observeTrackedClients);

        var observerId = ObserversState.AllocateObserverId();
        var entry = new ObserverEntry(observeInFlight, observeQueueDepth, observeTrackedClients);
        lock (ObserverGate)
            Observers[observerId] = entry;

        if (!ObserversState.TryRegisterObservers())
            return new ObserverRegistration(observerId);

        _ = ServerMeterRegistry.Meter.CreateObservableGauge(
            "squirix_backpressure_in_flight",
            static () => new Measurement<int>(Aggregate(static e => e.ObserveInFlight())),
            description: "Current number of admitted in-flight requests");
        _ = ServerMeterRegistry.Meter.CreateObservableGauge(
            "squirix_backpressure_queue_depth",
            static () => new Measurement<int>(Aggregate(static e => e.ObserveQueueDepth())),
            description: "Current number of requests waiting for admission");
        _ = ServerMeterRegistry.Meter.CreateObservableGauge(
            "squirix_backpressure_tracked_clients",
            static () => new Measurement<int>(Aggregate(static e => e.ObserveTrackedClients())),
            description: "Current number of client buckets tracked for backpressure state");

        return new ObserverRegistration(observerId);
    }

    private static int Aggregate(Func<ObserverEntry, int> selector)
    {
        lock (ObserverGate)
        {
            var count = Observers.Count;
            if (count is 0)
            {
                Array.Clear(_snapshotBuffer);
                return 0;
            }

            if (_snapshotBuffer.Length < count)
                _snapshotBuffer = new ObserverEntry[count];

            var copied = 0;
            foreach (var entry in Observers.Values)
                _snapshotBuffer[copied++] = entry;
            Array.Clear(_snapshotBuffer, copied, _snapshotBuffer.Length - copied);

            var total = 0;
            for (var i = 0; i < copied; i++)
            {
                try
                {
                    total += selector(_snapshotBuffer[i]);
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

    private sealed class ObserverRegistration : IDisposable
    {
        private readonly long _observerId;
        private int _disposed;

        internal ObserverRegistration(long observerId)
        {
            _observerId = observerId;
        }

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) is not 0)
                return;

            lock (ObserverGate)
                _ = Observers.Remove(_observerId);
        }
    }

    private sealed class ObserverState
    {
        private readonly MutableInt64 _nextObserverId = new();

        private readonly MutableInt32 _registered = new();

        internal long AllocateObserverId() => Interlocked.Increment(ref _nextObserverId.Value);

        internal bool TryRegisterObservers() => Interlocked.Exchange(ref _registered.Value, 1) is 0;
    }
}
