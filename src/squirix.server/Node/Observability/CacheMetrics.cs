using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Instruments for logical cache operation metrics.</summary>
[Immutable]
internal sealed class CacheMetrics
{
    internal const string OperationDurationName = "squirix_op_latency_seconds";
    internal const string OperationsTotalName = "squirix_ops_total";

    private readonly Histogram<double> _operationDurationSeconds;
    private readonly Counter<long> _operationsTotal;

    internal CacheMetrics(Meter meter)
    {
        _operationDurationSeconds = meter.CreateHistogram<double>(OperationDurationName, "s", "Logical cache operation duration");
        _operationsTotal = meter.CreateCounter<long>(OperationsTotalName, "{operation}", "Logical cache operations by operation and result");
    }

    /// <summary>Gets a value indicating whether at least one operation instrument has a listener.</summary>
    internal bool Enabled => _operationsTotal.Enabled || _operationDurationSeconds.Enabled;

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
