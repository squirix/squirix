using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

[Immutable]
internal sealed class ServerRpcTimeoutMetrics
{
    internal ServerRpcTimeoutMetrics(Meter meter)
    {
        TimeoutsTotal = new ServerCounter3Labels(meter.CreateCounter<long>("squirix_rpc_timeouts_total"), "peer", "scope", "kind");
    }

    internal ServerCounter3Labels TimeoutsTotal { get; }
}
