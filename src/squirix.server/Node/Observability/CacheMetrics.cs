using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Instruments for logical cache operation metrics.</summary>
[Immutable]
internal sealed class CacheMetrics
{
    private readonly Histogram<double> _operationDurationSeconds;
    private readonly Counter<long> _operationsTotal;

    internal CacheMetrics(Meter meter)
    {
        _operationDurationSeconds = meter.CreateHistogram<double>("squirix_op_latency_seconds", "s", "Logical cache operation duration");
        _operationsTotal = meter.CreateCounter<long>("squirix_ops_total", "{operation}", "Logical cache operations by operation and result");
    }

    internal void RecordOperation(string cacheName, string operation, string result, double durationSeconds)
    {
        var tags = new TagList
        {
            { "cache", cacheName },
            { "operation", operation },
            { "result", result },
        };

        _operationsTotal.Add(1, in tags);
        _operationDurationSeconds.Record(durationSeconds, in tags);
    }
}
