using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Attributes;

namespace Squirix.Server.Node.Observability;

internal static class ServerCallPolicyMetrics
{
    private static readonly Histogram1Label BackoffSeconds = new(ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_backoff_seconds"), "peer");
    private static readonly Counter1Label BackoffsTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_backoffs_total"), "peer");
    private static readonly Counter1Label DrainRejectsTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_drain_rejects_total"), "peer");
    private static readonly Histogram1Label QueueWaitSeconds = new(ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_queue_wait_seconds"), "peer");
    private static readonly Counter2Labels RetriesTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_retries_total"), "peer", "reason");

    internal static void IncrementBackoffLabel(string peer, int increment) => BackoffsTotal.WithLabels(peer).Inc(increment);

    internal static void IncrementDrainRejectsTotal(string peer, int increment) => DrainRejectsTotal.WithLabels(peer).Inc(increment);

    internal static void IncrementRetriesTotal(string peer, string reason, int increment = 1) => RetriesTotal.WithLabels(peer, reason).Inc(increment);

    internal static void ObserveBackoffSeconds(string peer, TimeSpan value) => BackoffSeconds.Observe(peer, value);

    internal static void ObserveQueueWaitSeconds(string peer, TimeSpan value) => QueueWaitSeconds.Observe(peer, value);

    [Immutable]
    private sealed record Counter1Label(Counter<long> Counter, string Key1)
    {
        internal ServerCounterLabelBinding WithLabels(string v1) => new(Counter, Key1, v1, "scope", "policy");
    }

    [Immutable]
    private sealed record Counter2Labels(Counter<long> Counter, string Key1, string Key2)
    {
        internal ServerCounterLabelBinding WithLabels(string v1, string v2) => new(Counter, Key1, v1, Key2, v2);
    }

    [Immutable]
    private sealed record Histogram1Label(Histogram<double> Histogram, string Key1)
    {
        internal void Observe(string v1, TimeSpan value)
        {
            var tags = new TagList
            {
                { Key1, v1 },
            };
            Histogram.Record(value.TotalSeconds, in tags);
        }
    }
}
