using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal static class CallPolicyMetrics
{
    internal static readonly Histogram1Label BackoffSeconds = new(MeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_backoff_seconds"), "peer");
    internal static readonly Counter1Label BackoffsTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_backoffs_total"), "peer");
    internal static readonly Counter1Label DrainRejectsTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_drain_rejects_total"), "peer");
    internal static readonly Histogram1Label QueueWaitSeconds = new(MeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_queue_wait_seconds"), "peer");
    internal static readonly Counter2Labels RetriesTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_retries_total"), "peer", "reason");

    internal sealed record Counter1Label
    {
        private readonly Counter<long> _ctr;
        private readonly string _k1;

        public Counter1Label(Counter<long> ctr, string k1)
        {
            _ctr = ctr;
            _k1 = k1;
        }

        internal CounterLabelBinding WithLabels(string v1) => new(_ctr, _k1, v1, "scope", "policy");
    }

    internal sealed record Counter2Labels
    {
        private readonly Counter<long> _ctr;
        private readonly string _k1;
        private readonly string _k2;

        public Counter2Labels(Counter<long> ctr, string k1, string k2)
        {
            _ctr = ctr;
            _k1 = k1;
            _k2 = k2;
        }

        internal CounterLabelBinding WithLabels(string v1, string v2) => new(_ctr, _k1, v1, _k2, v2);
    }

    internal sealed record Histogram1Label
    {
        private readonly Histogram<double> _histogram;
        private readonly string _k1;

        public Histogram1Label(Histogram<double> histogram, string k1)
        {
            _histogram = histogram;
            _k1 = k1;
        }

        internal void Observe(string v1, TimeSpan value)
        {
            var tags = new TagList
            {
                { _k1, v1 },
            };
            _histogram.Record(value.TotalSeconds, in tags);
        }
    }
}
