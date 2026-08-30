using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>Registers this node's idempotency store metrics with the host-scoped meter and removes registration on shutdown.</summary>
[Immutable]
internal sealed class IdempotencyMetricsService : IHostedService
{
    private readonly IdempotencyMetrics _metrics;
    private readonly IdempotencyMetricRegistration _registration;

    public IdempotencyMetricsService(TopologyOptions cluster, RpcMutationIdempotencyStore store, IdempotencyMetrics metrics)
    {
        _metrics = metrics;
        _registration = new IdempotencyMetricRegistration(cluster.NodeId, () => store.RecordCount);
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        _metrics.Register(_registration);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        _metrics.Unregister(_registration);
        return Task.CompletedTask;
    }
}
