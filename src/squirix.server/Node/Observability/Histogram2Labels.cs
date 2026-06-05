using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal readonly struct Histogram2Labels
{
    private readonly Histogram<double> _h;
    private readonly string _k1;
    private readonly string _k2;

    public Histogram2Labels(Histogram<double> h, string k1, string k2)
    {
        _h = h;
        _k1 = k1;
        _k2 = k2;
    }

    public HistogramLabelBinding WithLabels(string v1, string v2) => new(_h, _k1, v1, _k2, v2);
}
