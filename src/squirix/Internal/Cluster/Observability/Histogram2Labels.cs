using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal sealed record Histogram2Labels(Histogram<double> Histogram, string Key1, string Key2)
{
    internal HistogramLabelBinding WithLabels(string v1, string v2) => new(Histogram, Key1, v1, Key2, v2);
}
