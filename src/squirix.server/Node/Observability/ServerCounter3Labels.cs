using System.Diagnostics.Metrics;
using Squirix.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed record ServerCounter3Labels(Counter<long> Counter, string Key1, string Key2, string Key3)
{
    internal ServerLabelBinding WithLabels(string v1, string v2, string v3) => new(Counter, Key1, v1, Key2, v2, Key3, v3);
}
