using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Metrics for the server-side inter-node client pool.
/// </summary>
internal static class ClientPoolMetrics
{
    private static readonly Counter<long> DisposalsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_peer_pool_disposals_total");
    private static readonly Counter<long> WarmupsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_peer_pool_warmups_total");

    public static void AddDisposal() => DisposalsTotalCtr.Add(1);

    public static void AddWarmup() => WarmupsTotalCtr.Add(1);
}
