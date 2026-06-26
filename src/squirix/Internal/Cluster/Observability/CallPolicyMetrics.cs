using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal static class CallPolicyMetrics
{
    private static readonly Histogram1Label BackoffSeconds = new(MeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_backoff_seconds"), "peer");
    private static readonly Counter1Label BackoffsTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_backoffs_total"), "peer");
    private static readonly Counter1Label DrainRejectsTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_drain_rejects_total"), "peer");
    private static readonly Histogram1Label QueueWaitSeconds = new(MeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_queue_wait_seconds"), "peer");
    private static readonly Counter2Labels RetriesTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_retries_total"), "peer", "reason");

    internal static void IncrementBackoffLabel(string peer, int increment) => BackoffsTotal.WithLabels(peer).Inc(increment);

    internal static void IncrementDrainRejectsTotal(string peer, int increment) => DrainRejectsTotal.WithLabels(peer).Inc(increment);

    internal static void IncrementRetriesTotal(string peer, string reason, int increment = 1) => RetriesTotal.WithLabels(peer, reason).Inc(increment);

    internal static void ObserveBackoffSeconds(string peer, TimeSpan value) => BackoffSeconds.Observe(peer, value);

    internal static void ObserveQueueWaitSeconds(string peer, TimeSpan value) => QueueWaitSeconds.Observe(peer, value);

    private sealed record Counter1Label(Counter<long> Counter, string Key1)
    {
        internal CounterLabelBinding WithLabels(string v1) => new(Counter, Key1, v1, "scope", "policy");
    }

    private sealed record Counter2Labels(Counter<long> Counter, string Key1, string Key2)
    {
        internal CounterLabelBinding WithLabels(string v1, string v2) => new(Counter, Key1, v1, Key2, v2);
    }

    private sealed record Histogram1Label(Histogram<double> Histogram, string Key1)
    {
        internal void Observe(string v1, TimeSpan value)
        {
            var tags = new TagList
            {
                { Key1, v1 },
            };
            _histogram.Record(value.TotalSeconds, in tags);
        }
    }
}
