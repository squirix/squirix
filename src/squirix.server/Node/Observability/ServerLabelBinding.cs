using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed record ServerLabelBinding(Counter<long> Counter, string Key1, string Value1, string Key2, string Value2, string Key3, string Value3)
{
    internal void Inc() => Inc(1);

    internal void Inc(long value)
    {
        var tags = new TagList
        {
            { Key1, Value1 },
            { Key2, Value2 },
            { Key3, Value3 },
        };
        Counter.Add(value, in tags);
    }
}
