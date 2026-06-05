using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal readonly struct HistogramLabelBinding
{
    private readonly Histogram<double> _h;
    private readonly string _k1;
    private readonly string _k2;
    private readonly string _v1;
    private readonly string _v2;

    public HistogramLabelBinding(Histogram<double> h, string k1, string v1, string k2, string v2)
    {
        _h = h;
        _k1 = k1;
        _v1 = v1;
        _k2 = k2;
        _v2 = v2;
    }

    public void Observe(double value)
    {
        var tags = new TagList
        {
            { _k1, _v1 },
            { _k2, _v2 },
        };
        _h.Record(value, tags);
    }
}
