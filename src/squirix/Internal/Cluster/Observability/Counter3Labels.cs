using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal readonly struct Counter3Labels
{
    private readonly Counter<long> _ctr;
    private readonly string _k1;
    private readonly string _k2;
    private readonly string _k3;

    public Counter3Labels(Counter<long> ctr, string k1, string k2, string k3)
    {
        _ctr = ctr;
        _k1 = k1;
        _k2 = k2;
        _k3 = k3;
    }

    public LabelBinding WithLabels(string v1, string v2, string v3) => new(_ctr, _k1, v1, _k2, v2, _k3, v3);
}
