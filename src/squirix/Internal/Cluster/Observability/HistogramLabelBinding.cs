using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal sealed record HistogramLabelBinding(Histogram<double> Histogram, string Key1, string Value1, string Key2, string Value2)
{
    internal void Observe(double value)
    {
        var tags = new TagList
        {
            { Key1, Value1 },
            { Key2, Value2 },
        };
        _h.Record(value, in tags);
    }
}
