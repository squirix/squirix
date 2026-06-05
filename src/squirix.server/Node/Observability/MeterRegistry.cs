using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal static class MeterRegistry
{
    public static readonly Meter Meter = new("Squirix");
}
