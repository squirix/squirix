using System;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

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
    private readonly ILogger<JournalMetricsExporterService> _log;
    private readonly PersistenceOptions _opt;

    private readonly IOptionsMonitor<JournalMetricsExporterOptions> _options;

    private long _segments;

    private long _sizeBytes;

    public JournalMetricsExporterService(PersistenceOptions opt, IOptionsMonitor<JournalMetricsExporterOptions> options, TopologyOptions cluster, ILogger<JournalMetricsExporterService> log)
    {
        _opt = opt;
        _options = options;
        _log = log ?? throw new ArgumentNullException(nameof(log));
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
    private static void VolatileWrite(long value, ref long location) => Interlocked.Exchange(ref location, value);

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
            VolatileWrite(0, ref _segments);
            VolatileWrite(0, ref _sizeBytes);
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
            catch (IOException ex)
            {
                // Best-effort metrics scan: transient per-file IO failures should not stop gauge refresh.
                LogManager.JournalMetricFileProbeFailed(_log, ex, f);
            }
            catch (UnauthorizedAccessException ex)
            {
                // Best-effort metrics scan: transient per-file IO failures should not stop gauge refresh.
                LogManager.JournalMetricFileProbeFailed(_log, ex, f);
            }
        }

        VolatileWrite(length, ref _segments);
        VolatileWrite(total, ref _sizeBytes);
    }
}
