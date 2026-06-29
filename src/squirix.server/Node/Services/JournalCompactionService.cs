using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

internal sealed class JournalCompactionService<T> : BackgroundService, IJournalCompactionStatus
{
    private readonly IExclusiveMaintenanceExecutor _journalMaintenance;
    private readonly ILogger<JournalCompactionService<T>> _log;
    private readonly ManifestStore _manifest;
    private readonly string _nodeId;
    private readonly JournalCompactionOptions _opt;
    private readonly PersistenceOptions _persistence;
    private readonly SnapshotCoordinator<T> _snap;
    private readonly ISnapshotReader _snapshotReader;
    private readonly TimeProvider _timeProvider;
    private readonly EventHandler<SnapshotCompletedEventArgs> _onSnapshotCompleted;
    private int _consecutiveFailures;
    private int _inFlight;
    private int _snapshotSubscriptionState;

    public JournalCompactionService(
        ILogger<JournalCompactionService<T>> log,
        IOptions<JournalCompactionOptions> opt,
        SnapshotCoordinator<T> snap,
        IExclusiveMaintenanceExecutor journalMaintenance,
        ManifestStore manifest,
        ISnapshotReader snapshotReader,
        IOptions<PersistenceOptions> persistence,
        ClusterConfig cluster,
        TimeProvider? timeProvider = null)
    {
        _log = log;
        _snap = snap;
        _journalMaintenance = journalMaintenance;
        _manifest = manifest;
        _snapshotReader = snapshotReader;
        _nodeId = cluster.NodeId;
        _opt = opt.Value;
        _persistence = persistence.Value;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _onSnapshotCompleted = OnSnapshotCompleted;
    }

    public bool IsInFlight => Volatile.Read(ref _inFlight) is not 0;

    public DateTime LastRunUtc { get; private set; } = DateTime.MinValue;

    public CompactionState State { get; private set; } = CompactionState.Idle;

    public override Task StartAsync(CancellationToken cancellationToken)
    {
        if (_opt.Enabled)
            SubscribeSnapshotCompleted();

        return base.StartAsync(cancellationToken);
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => !_opt.Enabled ? Task.CompletedTask : RunLoopAsync(stoppingToken);

    private void ChangeState(CompactionState next)
    {
        var prev = State;
        if (prev == next)
            return;

        State = next;
        LogManager.CompactionStateChanged(_log, prev, next);
    }

    private async Task<AttemptResult> MaybeCompactAsync(ManifestState.SnapshotRef? snapshotHint, CancellationToken cancellationToken)
    {
        if (Interlocked.CompareExchange(ref _inFlight, 1, 0) is not 0)
            return AttemptResult.Skipped; // already running, skip

        try
        {
            if (DateTime.UtcNow - LastRunUtc < _opt.MinGap)
                return AttemptResult.Skipped;

            var m = await _manifest.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var replayFromSegment = snapshotHint?.ReplayFromJournalSegment ?? m.LastSnapshot?.ReplayFromJournalSegment ?? 0;
            var snapshotIndex = snapshotHint?.Index ?? m.LastSnapshot?.Index ?? 0;
            if (replayFromSegment <= 0 || !TailLargeEnough(replayFromSegment, out var segments, out var bytes))
                return AttemptResult.Skipped;

            return await RunCompactionAsync(snapshotIndex, replayFromSegment, segments, bytes, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return AttemptResult.Skipped;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or InvalidDataException)
        {
            return RecordCompactionFailure();
        }
        finally
        {
            Volatile.Write(ref _inFlight, 0);
        }
    }

    private void OnSnapshotCompleted(object? sender, SnapshotCompletedEventArgs e) => _ = MaybeCompactAsync(e.SnapshotRef, CancellationToken.None);

    private AttemptResult RecordCompactionFailure()
    {
        _consecutiveFailures++;
        ChangeState(CompactionState.Failed);
        LogManager.CompactionFailed(_log);
        return AttemptResult.Failed;
    }

    private async Task<AttemptResult> RunCompactionAsync(int snapshotIndex, int replayFromSegment, int segments, long bytes, CancellationToken cancellationToken)
    {
        using var activity = ActivitySourceHolder.StartInternal("journal.compact");
        _ = activity?.SetTag("compaction.snapshot_index", snapshotIndex);
        _ = activity?.SetTag("compaction.replay_from_journal_segment", replayFromSegment);
        _ = activity?.SetTag("compaction.tail_segments", segments);
        _ = activity?.SetTag("compaction.tail_bytes", bytes);

        ChangeState(CompactionState.Running);
        LogManager.CompactionStart(_log, snapshotIndex, segments, bytes);

        var sw = Stopwatch.StartNew();
        var resultLabel = "failure";
        try
        {
            await _journalMaintenance.ExecuteMaintenanceExclusiveAsync(
                ct => new ValueTask(JournalCompactor.CompactAsync(_persistence, _manifest, _snapshotReader, ct)),
                cancellationToken).ConfigureAwait(false);
            resultLabel = "success";
        }
        finally
        {
            sw.Stop();
            try
            {
                CompactionMetrics.DurationSeconds.WithLabels(_nodeId, resultLabel).Observe(sw.Elapsed.TotalSeconds);
            }
            catch (InvalidOperationException)
            {
                // Metrics emission is best-effort and must not fail compaction flow.
            }

            _ = activity?.SetTag("compaction.result", resultLabel);
            _ = activity?.SetTag("compaction.duration_ms", sw.Elapsed.TotalMilliseconds);
        }

        LastRunUtc = DateTime.UtcNow;
        _consecutiveFailures = 0;
        LogManager.CompactionDone(_log, LastRunUtc);
        ChangeState(CompactionState.Waiting);
        return AttemptResult.Succeeded;
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Base waiting state between checks
                ChangeState(CompactionState.Waiting);

                // Jitter next wake-up to avoid thundering herd across nodes
                var baseGap = _opt.MinGap <= TimeSpan.Zero ? TimeSpan.FromSeconds(10) : _opt.MinGap;
                var maxJitterMs = Math.Clamp(baseGap.TotalMilliseconds * 0.1, 50d, 10_000d);
                var jitterOffsetMs = ((RandomNumberGenerator.GetInt32(0, int.MaxValue) * (2d / int.MaxValue)) - 1d) * maxJitterMs;
                var delay = baseGap + TimeSpan.FromMilliseconds(jitterOffsetMs);
                await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);

                var res = await MaybeCompactAsync(null, cancellationToken).ConfigureAwait(false);

                if (res is not AttemptResult.Failed)
                    continue;

                // Exponential backoff with full jitter
                ChangeState(CompactionState.BackingOff);
                var pow = Math.Min(_consecutiveFailures, 10); // cap exponent
                var maxDelay = TimeSpan.FromSeconds(Math.Min(60, Math.Pow(2, pow))); // up to 60s
                var maxBackoffMs = Math.Max(10d, maxDelay.TotalMilliseconds);
                var backoffMs = RandomNumberGenerator.GetInt32(0, int.MaxValue) * (maxBackoffMs / int.MaxValue);
                var backoff = TimeSpan.FromMilliseconds(backoffMs);
                LogManager.CompactionBackoff(_log, _consecutiveFailures, Convert.ToInt32(backoffMs));
                await Task.Delay(backoff, _timeProvider, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Background compaction loop exits when the host token is Canceled; not an error for this service.
        }
        finally
        {
            UnsubscribeSnapshotCompleted();
            ChangeState(CompactionState.Idle);
        }
    }

    private void SubscribeSnapshotCompleted()
    {
        if (Interlocked.Exchange(ref _snapshotSubscriptionState, 1) is not 0)
            return;

        _snap.SnapshotCompleted += _onSnapshotCompleted;
    }

    private bool TailLargeEnough(int replayFromSegment, out int segments, out long bytes)
    {
        segments = 0;
        bytes = 0;

        foreach (var segment in JournalReader.EnumerateSegments(_persistence.DataDir, Math.Max(1, replayFromSegment)))
        {
            if (!File.Exists(segment.Path))
                continue;
            if (segment.Index < replayFromSegment)
                continue;

            segments++;
            bytes += new FileInfo(segment.Path).Length;
        }

        return segments >= _opt.MinTailSegments || bytes >= _opt.MinTailBytes;
    }

    private void UnsubscribeSnapshotCompleted()
    {
        if (Interlocked.Exchange(ref _snapshotSubscriptionState, 0) is 0)
            return;

        _snap.SnapshotCompleted -= _onSnapshotCompleted;
    }
}
