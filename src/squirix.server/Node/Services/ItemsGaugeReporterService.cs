using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Node.Services;

internal sealed class ItemsGaugeReporterService : BackgroundService
{
    private readonly ILocalCacheStats _stats;

    internal ItemsGaugeReporterService(ILocalCacheStats stats)
    {
        _stats = stats;
        _ = ServerMeterRegistry.Meter.CreateObservableGauge("squirix_items_total", ObserveCount, description: "Number of items in local cache");
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;

    private Measurement<long> ObserveCount() => new(_stats.EntryCount);
}
