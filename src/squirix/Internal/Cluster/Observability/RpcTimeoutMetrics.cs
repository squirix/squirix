namespace Squirix.Internal.Cluster.Observability;

internal static class RpcTimeoutMetrics
{
    internal static readonly Counter3Labels TimeoutsTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_rpc_timeouts_total"), "peer", "scope", "kind");
}
