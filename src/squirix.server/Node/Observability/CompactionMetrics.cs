using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed class CompactionMetrics
{
    internal CompactionMetrics(Meter meter)
    {
        DurationSeconds = new ServerHistogram2Labels(meter.CreateHistogram<double>("squirix_compaction_duration_seconds"), "node", "result");
    }

    /// <summary>Gets the compaction duration histogram. Labels: node, result (success|failure).</summary>
    internal ServerHistogram2Labels DurationSeconds { get; }
}
