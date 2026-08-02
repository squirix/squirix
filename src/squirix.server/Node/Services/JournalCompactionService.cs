using System;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
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
    private readonly EventHandler<CompletedEventArgs> _onSnapshotCompleted;
    private readonly JournalCompactionOptions _opt;
    private readonly PersistenceOptions _persistence;
    private readonly Coordinator _snap;
    private readonly ISnapshotReader _snapshotReader;
    private readonly TimeProvider _timeProvider;
    private int _consecutiveFailures;
    private int _inFlight;
    private SnapshotRef? _pendingSnapshotHint;
    private int _snapshotSubscriptionState;
    private TaskCompletionSource? _wake;

    internal JournalCompactionService(ILogger<JournalCompactionService<T>> log, IOptions<JournalCompactionOptions> opt, JournalCompactionDependencies deps)
    {
        _log = log ?? throw new ArgumentNullException(nameof(log));
        ArgumentNullException.ThrowIfNull(opt);
        _opt = opt.Value;
        ArgumentNullException.ThrowIfNull(deps);
        _snap = deps.Snapshot;
        _journalMaintenance = deps.JournalMaintenance;
        _manifest = deps.Manifest;
        _snapshotReader = deps.SnapshotReader;
        _nodeId = deps.Cluster.NodeId;
        _persistence = deps.Persistence;
        _timeProvider = deps.TimeProvider;
        _onSnapshotCompleted = OnSnapshotCompleted;
    }

    public bool IsInFlight => Volatile.Read(ref _inFlight) is not 0;

    public DateTime LastRunUtc { get; private set; } = DateTime.MinValue;

    public RunState State { get; private set; } = RunState.Idle;

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_opt.Enabled)
            return Task.CompletedTask;
        SubscribeSnapshotCompleted();
        return RunLoopAsync(stoppingToken);
    }

    private static string StateName(RunState state) => state switch
    {
        RunState.Idle => nameof(RunState.Idle),
        RunState.Waiting => nameof(RunState.Waiting),
        RunState.Running => nameof(RunState.Running),
        RunState.BackingOff => nameof(RunState.BackingOff),
        RunState.Failed => nameof(RunState.Failed),
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unsupported enum value."),
    };

    private void ChangeState(RunState next)
    {
        var prev = State;
        if (prev == next)
            return;

        State = next;
        if (!_log.IsEnabled(LogLevel.Debug))
            return;
        var prevName = StateName(prev);
        var nextName = StateName(next);
        LogManager.CompactionStateChanged(_log, prevName, nextName);
    }

    private async Task<AttemptResult> MaybeCompactAsync(SnapshotRef? snapshotHint, CancellationToken cancellationToken)
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

    private void OnSnapshotCompleted(object? sender, CompletedEventArgs e)
    {
        // Queue onto the hosted loop instead of fire-and-forget so StopAsync awaits in-flight work
        // and CI thread-pool delay cannot strand the snapshot-triggered attempt.
        Volatile.Write(ref _pendingSnapshotHint, e.SnapshotRef);
        var wake = Volatile.Read(ref _wake);
        if (wake is not null)
            _ = wake.TrySetResult();
    }

    private AttemptResult RecordCompactionFailure()
    {
        _consecutiveFailures++;
        ChangeState(RunState.Failed);
        LogManager.CompactionFailed(_log);
        return AttemptResult.Failed;
    }

    private async Task<AttemptResult> RunCompactionAsync(int snapshotIndex, int replayFromSegment, int segments, long bytes, CancellationToken cancellationToken)
    {
        using var activity = ActivitySourceHolder.StartInternal("journal.compact");
        _ = activity?.SetTag("compaction.snapshot_index", ActivityTagValues.Int32(snapshotIndex));
        _ = activity?.SetTag("compaction.replay_from_journal_segment", ActivityTagValues.Int32(replayFromSegment));
        _ = activity?.SetTag("compaction.tail_segments", ActivityTagValues.Int32(segments));
        _ = activity?.SetTag("compaction.tail_bytes", ActivityTagValues.Int64(bytes));

        ChangeState(RunState.Running);
        LogManager.CompactionStart(_log, snapshotIndex, segments, bytes);

        var started = Stopwatch.GetTimestamp();
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
            var elapsed = Stopwatch.GetElapsedTime(started);
            CompactionMetrics.DurationSeconds.WithLabels(_nodeId, resultLabel).Observe(elapsed.TotalSeconds);

            _ = activity?.SetTag("compaction.result", resultLabel);
            _ = activity?.SetTag("compaction.duration_ms", ActivityTagValues.Double(elapsed.TotalMilliseconds));
        }

        LastRunUtc = DateTime.UtcNow;
        _consecutiveFailures = 0;
        LogManager.CompactionDone(_log, LastRunUtc);
        ChangeState(RunState.Waiting);
        return AttemptResult.Succeeded;
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // Base waiting state between checks
                ChangeState(RunState.Waiting);
                await WaitForCompactionTurnAsync(cancellationToken).ConfigureAwait(false);

                var snapshotHint = Interlocked.Exchange(ref _pendingSnapshotHint, null);
                var res = await MaybeCompactAsync(snapshotHint, cancellationToken).ConfigureAwait(false);

                if (res is not AttemptResult.Failed)
                    continue;

                // Exponential backoff with full jitter
                ChangeState(RunState.BackingOff);
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
            ChangeState(RunState.Idle);
        }
    }

    private async Task WaitForCompactionTurnAsync(CancellationToken cancellationToken)
    {
        var wake = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref _wake, wake);
        try
        {
            // Snapshot completion may have queued work before the wake signal was published.
            if (Volatile.Read(ref _pendingSnapshotHint) is not null)
                return;

            // Jitter next wake-up to avoid thundering herd across nodes
            var baseGap = _opt.MinGap <= TimeSpan.Zero ? TimeSpan.FromSeconds(10) : _opt.MinGap;
            var maxJitterMs = Math.Clamp(baseGap.TotalMilliseconds * 0.1, 50d, 10_000d);
            var jitterOffsetMs = ((RandomNumberGenerator.GetInt32(0, int.MaxValue) * (2d / int.MaxValue)) - 1d) * maxJitterMs;
            var delay = baseGap + TimeSpan.FromMilliseconds(jitterOffsetMs);
            var delayTask = Task.Delay(delay, _timeProvider, cancellationToken);
            _ = await Task.WhenAny(delayTask, wake.Task).ConfigureAwait(false);
            if (delayTask.IsCompleted)
                await delayTask.ConfigureAwait(false);
        }
        finally
        {
            Volatile.Write(ref _wake, null);
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
        AccumulateTailStats(replayFromSegment, out segments, out bytes);
        return segments >= _opt.MinTailSegments || bytes >= _opt.MinTailBytes;
    }

    private void AccumulateTailStats(int replayFromSegment, out int segments, out long bytes)
    {
        segments = 0;
        bytes = 0;
        foreach (var segment in JournalReader.EnumerateSegments(_persistence.DataDir, Math.Max(1, replayFromSegment)))
        {
            if (!File.Exists(segment.Path) || segment.Index < replayFromSegment)
                continue;

            segments++;
            bytes += new FileInfo(segment.Path).Length;
        }
    }

    private void UnsubscribeSnapshotCompleted()
    {
        if (Interlocked.Exchange(ref _snapshotSubscriptionState, 0) is 0)
            return;

        _snap.SnapshotCompleted -= _onSnapshotCompleted;
    }
}
