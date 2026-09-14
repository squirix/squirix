using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.LocalCache;

namespace Squirix.Server.Node.Services;

internal sealed class ItemsGaugeReporterService : BackgroundService
{
    private readonly ILocalCacheStats _stats;

    internal ItemsGaugeReporterService(ILocalCacheStats stats, Meter meter)
    {
        _stats = stats;
        _ = meter.CreateObservableGauge("squirix_items_total", ObserveCount, description: "Number of items in local cache");
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Task.CompletedTask;

    private Measurement<long> ObserveCount() => new(_stats.EntryCount);
}
