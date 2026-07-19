using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>Registers this node's idempotency store metrics with the shared meter and removes registration on shutdown.</summary>
internal sealed class IdempotencyMetricsService : IHostedService
{
    private readonly IdempotencyMetricRegistration _registration;

    public IdempotencyMetricsService(TopologyOptions cluster, RpcMutationIdempotencyStore store)
    {
        _registration = new IdempotencyMetricRegistration(cluster.NodeId, () => store.RecordCount);
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        IdempotencyMetrics.Register(_registration);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        IdempotencyMetrics.Unregister(_registration);
        return Task.CompletedTask;
    }
}
