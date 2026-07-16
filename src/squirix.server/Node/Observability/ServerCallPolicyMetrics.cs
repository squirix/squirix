using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal static class ServerCallPolicyMetrics
{
    internal static readonly Histogram1Label BackoffSeconds = new(ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_backoff_seconds"), "peer");
    internal static readonly Counter1Label BackoffsTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_backoffs_total"), "peer");
    internal static readonly Counter1Label DrainRejectsTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_drain_rejects_total"), "peer");
    internal static readonly Histogram1Label QueueWaitSeconds = new(ServerMeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_queue_wait_seconds"), "peer");
    internal static readonly Counter2Labels RetriesTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_retries_total"), "peer", "reason");

    internal sealed record Counter1Label(Counter<long> Counter, string Key1)
    {
        internal ServerCounterLabelBinding WithLabels(string v1) => new(Counter, Key1, v1, "scope", "policy");
    }

    internal sealed record Counter2Labels(Counter<long> Counter, string Key1, string Key2)
    {
        internal ServerCounterLabelBinding WithLabels(string v1, string v2) => new(Counter, Key1, v1, Key2, v2);
    }

    internal sealed record Histogram1Label(Histogram<double> Histogram, string Key1)
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
