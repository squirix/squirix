namespace Squirix.Server.Node.Observability;

internal static class RpcTimeoutMetrics
{
    public static readonly Counter3Labels TimeoutsTotal = new(MeterRegistry.Meter.CreateCounter<long>("squirix_rpc_timeouts_total"), "peer", "scope", "kind");
}
