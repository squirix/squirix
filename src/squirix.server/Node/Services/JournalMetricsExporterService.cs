using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;

#pragma warning disable SA1005 // PVS-Studio False Alarm markers use //-VNNNN (no space after //).

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
    private const string JournalSegmentSearchPattern = $"{FilePrefixes.Journal}*{FileExtensions.Journal}";

    private readonly string _nodeId;
    private readonly PersistenceOptions _opt;

    private readonly IOptionsMonitor<JournalMetricsExporterOptions> _options;

    private long _segments;

    private long _sizeBytes;

    public JournalMetricsExporterService(PersistenceOptions opt, IOptionsMonitor<JournalMetricsExporterOptions> options, TopologyOptions cluster)
    {
        _opt = opt;
        _options = options;
        _nodeId = cluster.NodeId;

        _ = ServerMeterRegistry.Meter.CreateObservableGauge("squirix_journal_segments", ObserveSegments, description: "Number of journal segment files currently present on disk");

        _ = ServerMeterRegistry.Meter.CreateObservableGauge(
            "squirix_journal_size_bytes",
            ObserveSize,
            description: "Total size of journal segment files currently present on disk");
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long VolatileRead(ref long location) => Interlocked.Read(ref location);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void VolatileWrite(ref long location, long value) => Interlocked.Exchange(ref location, value);

    private Measurement<long> ObserveSegments()
    {
        var tags = new TagList
        {
            { "node", _nodeId },
        };
        return new Measurement<long>(VolatileRead(ref _segments), in tags);
    }

    private Measurement<long> ObserveSize()
    {
        var tags = new TagList
        {
            { "node", _nodeId },
        };
        return new Measurement<long>(VolatileRead(ref _sizeBytes), in tags);
    }

    private void RefreshFromDisk()
    {
        var dir = _opt.DataDir;
        if (!Directory.Exists(dir))
        {
            VolatileWrite(ref _segments, 0);
            VolatileWrite(ref _sizeBytes, 0);
            return;
        }

        var files = Directory.GetFiles(dir, JournalSegmentSearchPattern, SearchOption.TopDirectoryOnly);
        var length = files.LongLength;
        var total = 0L;
        foreach (var f in files)
        {
            try
            {
                total += new FileInfo(f).Length;
            }
            catch (IOException)
            {
                //-V5606 //-V3163
                // Best-effort metrics scan: transient per-file IO failures should not stop gauge refresh.
            }
            catch (UnauthorizedAccessException)
            {
                //-V5606 //-V3163
                // Best-effort metrics scan: transient per-file IO failures should not stop gauge refresh.
            }
        }

        VolatileWrite(ref _segments, length);
        VolatileWrite(ref _sizeBytes, total);
    }
}
