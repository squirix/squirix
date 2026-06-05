using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Exposes journal on-disk gauges via ObservableGauges:
/// - <c>squirix_journal_segments{node="..."}</c>: count of journal segment files
/// - <c>squirix_journal_size_bytes{node="..."}</c>: total size of journal segment files
/// The actual filesystem scan is done on a background interval, and the gauges
/// simply return the latest cached values to keep scrapes cheap.
/// </summary>
internal sealed class JournalMetricsExporterService : BackgroundService
{
    private readonly string _nodeId;
    private readonly PersistenceOptions _opt;

    private readonly IOptionsMonitor<JournalMetricsExporterOptions> _options;

    private long _segments;

    private long _sizeBytes;

    public JournalMetricsExporterService(PersistenceOptions opt, IOptionsMonitor<JournalMetricsExporterOptions> options, ClusterConfig cluster)
    {
        _opt = opt;
        _options = options;
        _nodeId = cluster.NodeId;

        _ = MeterRegistry.Meter.CreateObservableGauge("squirix_journal_segments", ObserveSegments, description: "Number of journal segment files currently present on disk");

        _ = MeterRegistry.Meter.CreateObservableGauge("squirix_journal_size_bytes", ObserveSize, description: "Total size of journal segment files currently present on disk");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Initial populate
        RefreshFromDisk();

        while (!stoppingToken.IsCancellationRequested)
        {
            var interval = _options.CurrentValue.Interval;
            if (interval <= TimeSpan.Zero)
                interval = TimeSpan.FromSeconds(5); // safety default

            try
            {
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }

            RefreshFromDisk();
        }
    }

    private static KeyValuePair<string, object?>[] TagsFor(string nodeId) => [new("node", nodeId)];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long VolatileRead(ref long location) => Interlocked.Read(ref location);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void VolatileWrite(ref long location, long value) => Interlocked.Exchange(ref location, value);

    private Measurement<long> ObserveSegments() => new(VolatileRead(ref _segments), TagsFor(_nodeId));

    private Measurement<long> ObserveSize() => new(VolatileRead(ref _sizeBytes), TagsFor(_nodeId));

    private void RefreshFromDisk()
    {
        var dir = _opt.DataDir;
        if (!Directory.Exists(dir))
        {
            VolatileWrite(ref _segments, 0);
            VolatileWrite(ref _sizeBytes, 0);
            return;
        }

        var files = Directory.GetFiles(dir, $"{StorageFilePrefixes.Journal}*{StorageFileExtensions.Journal}", SearchOption.TopDirectoryOnly);
        var length = files.LongLength;
        var total = 0L;
        foreach (var f in files)
        {
            try
            {
                total += new FileInfo(f).Length;
            }
            catch
            {
                // Best-effort metrics scan: transient per-file IO failures should not stop gauge refresh.
            }
        }

        VolatileWrite(ref _segments, length);
        VolatileWrite(ref _sizeBytes, total);
    }
}
