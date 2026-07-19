using System.Diagnostics.Metrics;

namespace Squirix.Internal.Cluster.Observability;

internal static class MeterRegistry
{
    internal static readonly Meter Meter = new("Squirix");
}
