using System.Diagnostics.Metrics;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Metrics for the server-side inter-node client pool.</summary>
[Immutable]
internal sealed class ServerClientPoolMetrics
{
    private readonly Counter<long> _disposalsTotalCtr;

    internal ServerClientPoolMetrics(Meter meter)
    {
        _disposalsTotalCtr = meter.CreateCounter<long>("squirix_peer_pool_disposals_total");
    }

    internal void AddDisposal() => _disposalsTotalCtr.Add(1);
}
