using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal static class ServerMeterRegistry
{
    internal static readonly Meter Meter = new("Squirix");
}
