using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal sealed record ServerHistogramLabelBinding(Histogram<double> Histogram, string Key1, string Value1, string Key2, string Value2)
{
    internal void Observe(double value)
    {
        var tags = new TagList
        {
            { Key1, Value1 },
            { Key2, Value2 },
        };
        Histogram.Record(value, in tags);
    }
}
