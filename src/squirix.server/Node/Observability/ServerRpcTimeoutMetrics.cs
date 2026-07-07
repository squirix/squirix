namespace Squirix.Server.Node.Observability;

internal static class ServerRpcTimeoutMetrics
{
    internal static readonly ServerCounter3Labels TimeoutsTotal = new(ServerMeterRegistry.Meter.CreateCounter<long>("squirix_rpc_timeouts_total"), "peer", "scope", "kind");
}
