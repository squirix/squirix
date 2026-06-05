using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal static class CallPolicyMetrics
{
    public static readonly Histogram1Label BackoffSeconds;
    public static readonly Counter1Label BackoffsTotal;
    public static readonly Counter1Label DrainRejectsTotal;
    public static readonly Histogram1Label QueueWaitSeconds;
    public static readonly Counter2Labels RetriesTotal;
    private static readonly Histogram<double> BackoffSecondsHist = MeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_backoff_seconds");
    private static readonly Counter<long> BackoffsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_backoffs_total");
    private static readonly Counter<long> DrainRejectsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_drain_rejects_total");
    private static readonly Histogram<double> QueueWaitSecondsHist = MeterRegistry.Meter.CreateHistogram<double>("squirix_call_policy_queue_wait_seconds");

    private static readonly Counter<long> RetriesTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_call_policy_retries_total");

    static CallPolicyMetrics()
    {
        RetriesTotal = new Counter2Labels(RetriesTotalCtr, "peer", "reason");
        DrainRejectsTotal = new Counter1Label(DrainRejectsTotalCtr, "peer");
        BackoffsTotal = new Counter1Label(BackoffsTotalCtr, "peer");
        BackoffSeconds = new Histogram1Label(BackoffSecondsHist, "peer");
        QueueWaitSeconds = new Histogram1Label(QueueWaitSecondsHist, "peer");
    }

    internal readonly struct Counter1Label
    {
        private readonly Counter<long> _ctr;
        private readonly string _k1;

        public Counter1Label(Counter<long> ctr, string k1)
        {
            _ctr = ctr;
            _k1 = k1;
        }

        public CounterLabelBinding WithLabels(string v1) => new(_ctr, _k1, v1, "scope", "policy");
    }

    internal readonly struct Counter2Labels
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

        public CounterLabelBinding WithLabels(string v1, string v2) => new(_ctr, _k1, v1, _k2, v2);
    }

    internal readonly struct Histogram1Label
    {
        private readonly Histogram<double> _histogram;
        private readonly string _k1;

        public Histogram1Label(Histogram<double> histogram, string k1)
        {
            _histogram = histogram;
            _k1 = k1;
        }

        public void Observe(string v1, TimeSpan value)
        {
            var tags = new TagList
            {
                { _k1, v1 },
            };
            _histogram.Record(value.TotalSeconds, tags);
        }
    }
}
