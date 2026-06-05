using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal readonly struct CounterLabelBinding
{
    private readonly Counter<long> _c;
    private readonly string _k1;
    private readonly string _k2;
    private readonly string _v1;
    private readonly string _v2;

    public CounterLabelBinding(Counter<long> c, string k1, string v1, string k2, string v2)
    {
        _c = c;
        _k1 = k1;
        _v1 = v1;
        _k2 = k2;
        _v2 = v2;
    }

    public void Inc(long value)
    {
        var tags = new TagList
        {
            { _k1, _v1 },
            { _k2, _v2 },
        };
        _c.Add(value, tags);
    }
}
