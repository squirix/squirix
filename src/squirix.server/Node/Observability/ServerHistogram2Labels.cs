using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed record ServerHistogram2Labels(Histogram<double> Histogram, string Key1, string Key2)
{
    internal ServerHistogramLabelBinding WithLabels(string v1, string v2) => new(Histogram, Key1, v1, Key2, v2);
}
