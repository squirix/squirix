using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

/// <summary>Metrics for the server-side inter-node client pool.</summary>
internal static class ServerClientPoolMetrics
{
    private static readonly Counter<long> DisposalsTotalCtr = ServerMeterRegistry.Meter.CreateCounter<long>("squirix_peer_pool_disposals_total");

    internal static void AddDisposal() => DisposalsTotalCtr.Add(1);
}
