using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed class ServerCallPolicyMetrics
{
    private readonly Histogram1Label _backoffSeconds;
    private readonly Counter1Label _backoffsTotal;
    private readonly Counter1Label _drainRejectsTotal;
    private readonly Histogram1Label _queueWaitSeconds;
    private readonly Counter2Labels _retriesTotal;

    internal ServerCallPolicyMetrics(Meter meter)
    {
        _backoffSeconds = new Histogram1Label(meter.CreateHistogram<double>("squirix_call_policy_backoff_seconds"), "peer");
        _backoffsTotal = new Counter1Label(meter.CreateCounter<long>("squirix_call_policy_backoffs_total"), "peer");
        _drainRejectsTotal = new Counter1Label(meter.CreateCounter<long>("squirix_call_policy_drain_rejects_total"), "peer");
        _queueWaitSeconds = new Histogram1Label(meter.CreateHistogram<double>("squirix_call_policy_queue_wait_seconds"), "peer");
        _retriesTotal = new Counter2Labels(meter.CreateCounter<long>("squirix_call_policy_retries_total"), "peer", "reason");
    }

    internal void IncrementBackoffLabel(string peer, int increment) => _backoffsTotal.WithLabels(peer).Inc(increment);

    internal void IncrementDrainRejectsTotal(string peer, int increment) => _drainRejectsTotal.WithLabels(peer).Inc(increment);

    internal void IncrementRetriesTotal(string peer, string reason, int increment = 1) => _retriesTotal.WithLabels(peer, reason).Inc(increment);

    internal void ObserveBackoffSeconds(string peer, TimeSpan value) => _backoffSeconds.Observe(peer, value);

    internal void ObserveQueueWaitSeconds(string peer, TimeSpan value) => _queueWaitSeconds.Observe(peer, value);

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
