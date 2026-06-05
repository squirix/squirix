using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Instruments for logical cache operation metrics.
/// </summary>
internal static class CacheMetrics
{
    private static readonly Histogram<double> OperationDurationSeconds = MeterRegistry.Meter.CreateHistogram<double>(
        "squirix_op_latency_seconds",
        "s",
        "Logical cache operation duration");

    private static readonly Counter<long> OperationsTotal = MeterRegistry.Meter.CreateCounter<long>(
        "squirix_ops_total",
        "{operation}",
        "Logical cache operations by operation and result");

    public static void RecordOperation(string cacheName, string operation, string result, double durationSeconds)
    {
        var tags = new TagList
        {
            { "cache", cacheName },
            { "operation", operation },
            { "result", result },
        };

        OperationsTotal.Add(1, tags);
        OperationDurationSeconds.Record(durationSeconds, tags);
    }
}
