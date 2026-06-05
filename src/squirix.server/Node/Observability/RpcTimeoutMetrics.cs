using System.Diagnostics.Metrics;

namespace Squirix.Server.Node.Observability;

internal static class RpcTimeoutMetrics
{
    public static readonly Counter3Labels TimeoutsTotal;

    private static readonly Counter<long> TimeoutsTotalCtr = MeterRegistry.Meter.CreateCounter<long>("squirix_rpc_timeouts_total");

    static RpcTimeoutMetrics()
    {
        TimeoutsTotal = new Counter3Labels(TimeoutsTotalCtr, "peer", "scope", "kind");
    }
}
