using System.Diagnostics;
using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed record ServerCounter3Labels(Counter<long> Counter, string Key1, string Key2, string Key3)
{
    internal void Add(string v1, string v2, string v3, long value = 1)
    {
        var tags = new TagList
        {
            { Key1, v1 },
            { Key2, v2 },
            { Key3, v3 },
        };
        Counter.Add(value, in tags);
    }
}
