using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed record ServerCounterLabelBinding(Counter<long> Counter, string Key1, string Value1, string Key2, string Value2)
{
    internal void Inc(long value)
    {
        var tags = new TagList
        {
            { Key1, Value1 },
            { Key2, Value2 },
        };
        Counter.Add(value, in tags);
    }
}
