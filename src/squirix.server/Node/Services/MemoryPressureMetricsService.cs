using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Registers this node's memory pressure metrics with the shared meter and removes registration on shutdown
/// so tests and short-lived hosts do not leave stale observable sources.
/// </summary>
internal sealed class MemoryPressureMetricsService : IHostedService
{
    private readonly MemoryPressureMetricRegistration _registration;

    public MemoryPressureMetricsService(ClusterConfig cluster, IMemoryUsageAccounting accounting, IMemoryPressureStateEvaluator evaluator)
    {
        _registration = new MemoryPressureMetricRegistration(cluster.NodeId, accounting, evaluator);
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        MemoryPressureMetrics.Register(_registration);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        MemoryPressureMetrics.Unregister(_registration);
        return Task.CompletedTask;
    }
}
