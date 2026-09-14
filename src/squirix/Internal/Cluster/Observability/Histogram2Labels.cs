using System.Diagnostics.Metrics;
using Squirix.Attributes;

namespace Squirix.Internal.Cluster.Observability;

[Immutable]
internal sealed record Histogram2Labels(Histogram<double> Histogram, string Key1, string Key2)
{
    internal HistogramLabelBinding WithLabels(string v1, string v2) => new(Histogram, Key1, v1, Key2, v2);
}
