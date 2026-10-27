using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>
/// Coordinates safe snapshot creation.
/// Background snapshots respect interval, volume, and memory-pressure throttles.
/// Concurrency: ensures at most one snapshot runs at a time using an interlocked flag.
/// Ordering guarantee vs writes: under the journal mutation gate we enqueue a journal checkpoint and record seqAtFlush = journal.NextSequence - 1; the checkpoint ack
/// is awaited after the gate is released and before the snapshot is written.
/// NextSequence is the next sequence to allocate, so seqAtFlush is the last allocated frame, already applied to memory at capture.
/// The snapshot reflects all effects of operations with Seq less or equal to seqAtFlush. Recovery will replay only operations with Seq > seqAtFlush.
/// Snapshot cut is two-phase: a brief barrier captures a consistent in-memory view under the journal mutation gate, then the checkpoint wait, serialization and
/// manifest I/O run outside the gate so a slow disk or a large snapshot does not stop-the-world block durable memory applies.
/// <see cref="SnapshotCompleted" /> is raised only after the journal mutation gate and the in-flight flag are released so subscribers can safely run work
/// that re-enters the writer.
/// </summary>
internal sealed class Coordinator
{
    private const int Idle = 0;

    private const int SnapshotRunning = 1;

    private readonly CaptureScratch _captureScratch = new();
    private readonly ISnapshotEntryCapture _entryCapture;
    private readonly IIdempotencySnapshotExporter _idempotency;
    private readonly Ledger _manifestStore;
    private readonly string _nodeId;
    private readonly ISnapshotWriter _snapWriter;
    private readonly ISnapshotTelemetry _telemetry;
    private readonly TimeProvider _timeProvider;
    private readonly TriggerState _triggerState;
    private int _exclusiveOwner;

    internal Coordinator(TriggerOptions opt, IJournalMetrics journal, CoordinatorDependencies deps, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(deps);
        _entryCapture = deps.EntryCapture;
        _snapWriter = deps.SnapWriter;
        _manifestStore = deps.Ledger;
        _idempotency = deps.Idempotency;
        _nodeId = deps.NodeId;
        _telemetry = deps.Telemetry;
        _timeProvider = timeProvider;
        _triggerState = new TriggerState(opt, journal, _timeProvider);
    }

    public event EventHandler<CompletedEventArgs>? SnapshotCompleted;

    internal bool IsInFlight => Volatile.Read(ref _exclusiveOwner) == SnapshotRunning;

    /// <summary>Takes a snapshot when the triggers allow it.</summary>
    /// <param name="journal">The journal to cut.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> when a snapshot was published; <see langword="false" /> when the triggers, an in-flight run, or a moved manifest left nothing to publish.</returns>
    internal async ValueTask<bool> SnapshotAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        if (!_triggerState.ShouldTrigger(IsInFlight, journal.AtHighWater, journal.CurrentSegmentIndex))
            return false;

        if (Interlocked.CompareExchange(ref _exclusiveOwner, SnapshotRunning, Idle) != Idle)
            return false;

        using var activity = _telemetry.BeginCreate();
        var started = Stopwatch.GetTimestamp();
        var result = "failure";
        SnapshotRef? published;
        try
        {
            var baseline = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            published = await journal.ExecuteSnapshotCutAsync(
                (Coordinator: this, Activity: activity, Journal: journal, Baseline: baseline),
                static async (state, _, ct) =>
                {
                    var captured = await state.Coordinator.CaptureSnapshotBundleAsync(state.Journal, ct).ConfigureAwait(false);
                    state.Activity?.SetTag("snapshot.items_count", InvariantDigitStrings.Format(captured.Items.Count));
                    return captured;
                },
                static (state, seqAtFlush, captured, ct) => state.Coordinator.PublishSnapshotAsync(state.Journal, seqAtFlush, captured, state.Baseline, state.Activity, ct),
                cancellationToken).ConfigureAwait(false);

            result = published == null ? "aborted" : "success";
            _triggerState.ClearLatencyThrottle();
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(started);
            _telemetry.RecordDuration(_nodeId, result, elapsed);

            activity?.SetTag("snapshot.result", result);
            activity?.SetTag("snapshot.duration_ms", InvariantDigitStrings.Format(elapsed.TotalMilliseconds));

            // The published snapshot owns its own copy; keeping the captured entries here would hold every one of them until the next capture.
            _captureScratch.Clear();
            Volatile.Write(ref _exclusiveOwner, Idle);
        }

        // Raised after the in-flight flag is released: a subscriber that starts work on the writer must not find this snapshot still in flight.
        if (published != null)
            SnapshotCompleted?.Invoke(this, new CompletedEventArgs(published));

        return published != null;
    }

    private static bool ManifestMovedSince(State baseline, State current) => current.LastSnapshot != baseline.LastSnapshot || current.CurrentJournal < baseline.CurrentJournal;

    private async ValueTask<CapturedSnapshotBundle> CaptureSnapshotBundleAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        _captureScratch.Clear();
        var utcNow = _timeProvider.GetUtcNow().UtcDateTime;
        await _entryCapture.CaptureEntriesAsync(_captureScratch.Items, utcNow, cancellationToken).ConfigureAwait(false);

        _idempotency.ExportSnapshot(_captureScratch.IdempotencyRecords, utcNow);
        return new CapturedSnapshotBundle(_captureScratch.Items, journal.CurrentSegmentIndex, journal.NextSequence, _captureScratch.IdempotencyRecords);
    }

    private async ValueTask<SnapshotRef?> PublishSnapshotAsync(
        IJournalCoordinator journal,
        ulong seqAtFlush,
        CapturedSnapshotBundle captured,
        State baseline,
        ISnapshotTraceScope? currentActivity,
        CancellationToken cancellationToken)
    {
        currentActivity?.SetTag("snapshot.seq_at_flush", InvariantDigitStrings.Format(seqAtFlush));

        var prev = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (ManifestMovedSince(baseline, prev))
            return null;

        var nextIndex = _snapWriter.AllocateIndex(prev.LastSnapshot?.Index ?? 0);
        currentActivity?.SetTag("snapshot.index", InvariantDigitStrings.Format(nextIndex));

        var path = await _snapWriter.WriteAsync(nextIndex, captured.Items, captured.IdempotencyRecordsAtFlush, cancellationToken).ConfigureAwait(false);
        currentActivity?.SetTag("snapshot.path", path);

        prev = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        if (ManifestMovedSince(baseline, prev))
            return null;

        var now = _timeProvider.GetUtcNow().UtcDateTime;
        var updated = new State
        {
            // The publishing runs outside the mutation-gate barrier, so a roll can be queued or committed
            // at any point around this write. These are floors: the manifest publisher raises them to the
            // manifest it holds when the write runs, so the manifest never moves CurrentJournal or
            // NextSequence backward and never falls behind the snapshot's own replay pointer.
            CurrentJournal = captured.ReplayFromJournalSegmentAtFlush,
            NextSequence = captured.NextSequenceAtFlush,
            LastSnapshot = new SnapshotRef
            {
                Index = nextIndex,
                CreatedUtc = now,
                LastAppliedSequence = seqAtFlush,
                ReplayFromJournalSegment = captured.ReplayFromJournalSegmentAtFlush,
            },
        };
        try
        {
            await _manifestStore.WriteSnapshotAsync(updated, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception) when (_manifestStore.PublishFailure is { } failure)
        {
            // The manifest outcome is unknown and the ledger refuses every later publish; stop here instead of capturing and writing a snapshot per tick.
            journal.FailJournalPipeline(failure);
            throw;
        }

        _triggerState.RecordSuccess(captured.ReplayFromJournalSegmentAtFlush);
        return updated.LastSnapshot;
    }

    [Immutable]
    private sealed record CapturedSnapshotBundle(
        List<(CacheKey Key, NodeCacheEntry<object?> Entry)> Items,
        int ReplayFromJournalSegmentAtFlush,
        ulong NextSequenceAtFlush,
        IReadOnlyList<PersistedIdempotencyRecord> IdempotencyRecordsAtFlush);

    [Immutable]
    private sealed class CaptureScratch
    {
        internal List<PersistedIdempotencyRecord> IdempotencyRecords { get; } = [];

        internal List<(CacheKey Key, NodeCacheEntry<object?> Entry)> Items { get; } = [];

        internal void Clear()
        {
            Items.Clear();
            IdempotencyRecords.Clear();
        }
    }

    /// <summary>
    /// Encapsulates snapshot trigger evaluation, latency-throttle tracking, and baseline bookkeeping.
    /// Callers pass the current in-flight flag so this type stays independent of the snapshot-in-flight
    /// state owned by <see cref="Coordinator" />.
    /// Evaluations and baseline updates are serialized on an internal gate so concurrent trigger checks
    /// cannot race on the trigger fields; only the CAS winner in <see cref="Coordinator" /> records success.
    /// </summary>
    private sealed class TriggerState
    {
        private readonly Lock _gate = new();
        private readonly IJournalMetrics _journal;
        private readonly TriggerOptions _opt;
        private readonly TimeProvider _timeProvider;
        private long _bytesAtLast;
        private long? _lastSnapshotTimestamp;
        private long? _latencyThrottleTimestamp;
        private long _opsAtLast;
        private int? _replayFromAtLast;

        internal TriggerState(TriggerOptions opt, IJournalMetrics journal, TimeProvider timeProvider)
        {
            _opt = opt;
            _journal = journal;
            _timeProvider = timeProvider;
        }

        /// <summary>Resets the latency throttle so the next evaluation may proceed normally.</summary>
        internal void ClearLatencyThrottle()
        {
            lock (_gate)
                _latencyThrottleTimestamp = null;
        }

        /// <summary>Records the journal baseline and the monotonic time of a successful snapshot.</summary>
        /// <param name="replayFromJournalSegment">The first journal segment recovery replays after the published snapshot.</param>
        internal void RecordSuccess(int replayFromJournalSegment)
        {
            lock (_gate)
            {
                _replayFromAtLast = replayFromJournalSegment;
                _lastSnapshotTimestamp = _timeProvider.GetTimestamp();
                _opsAtLast = _journal.AppendedOps;
                _bytesAtLast = _journal.AppendedBytes;
            }
        }

        /// <summary>Returns <see langword="true" /> when conditions are met to start a new snapshot.</summary>
        /// <param name="isInFlight">Whether a snapshot is already running on the coordinator.</param>
        /// <param name="atHighWater">Whether the journal reached its high-water mark.</param>
        /// <param name="currentSegmentIndex">The index of the active journal segment.</param>
        /// <returns><see langword="true" /> if a snapshot should be triggered; otherwise <see langword="false" />.</returns>
        /// <remarks>
        /// Every interval is measured on monotonic time, so a wall-clock step neither blocks nor hastens a snapshot. At the high-water mark a snapshot is due
        /// as soon as it can free a closed segment the last snapshot did not cover; that fill trigger ignores the min gap and the latency throttle, because
        /// waiting only fills the journal further.
        /// </remarks>
        internal bool ShouldTrigger(bool isInFlight, bool atHighWater, int currentSegmentIndex)
        {
            lock (_gate)
            {
                if (atHighWater && !isInFlight && (_replayFromAtLast is not { } replayFrom || currentSegmentIndex > replayFrom))
                    return true;

                var opsDelta = _journal.AppendedOps - _opsAtLast;
                var bytesDelta = _journal.AppendedBytes - _bytesAtLast;
                TimeSpan? sinceLast = _lastSnapshotTimestamp is { } last ? _timeProvider.GetElapsedTime(last) : null;
                var blocked = IsBlockedFromTriggering(sinceLast, isInFlight);
                var throttled = _opt.JournalGrowthThrottleBytes > 0 && bytesDelta < _opt.JournalGrowthThrottleBytes;
                return !blocked && !throttled && MeetsAnyTriggerThreshold(sinceLast, opsDelta, bytesDelta);
            }
        }

        private bool IsBlockedFromTriggering(TimeSpan? sinceLast, bool isInFlight)
        {
            var isWithinMinGap = sinceLast < _opt.MinGapBetweenSnapshots;
            return true switch
            {
                _ when IsLatencyThrottled() => true,
                _ when ShouldEnterLatencyThrottle() => true,
                _ when isWithinMinGap => true,
                _ => isInFlight,
            };
        }

        private bool IsLatencyThrottled() => _latencyThrottleTimestamp is { } since && _timeProvider.GetElapsedTime(since) < LatencyThrottleBackoff();

        private TimeSpan LatencyThrottleBackoff() => _opt.LatencyThrottleDuration <= TimeSpan.Zero ? TimeSpan.FromSeconds(10) : _opt.LatencyThrottleDuration;

        private bool MeetsAnyTriggerThreshold(TimeSpan? sinceLast, long opsDelta, long bytesDelta)
        {
            var anyActivity = opsDelta > 0 || bytesDelta > 0;
            var timeOk = _opt.SnapshotInterval > TimeSpan.Zero && (sinceLast == null || sinceLast >= _opt.SnapshotInterval) && anyActivity;
            var opsOk = _opt.SnapshotEveryNOps > 0 && opsDelta >= _opt.SnapshotEveryNOps;
            var bytesOk = _opt.SnapshotEveryNBytes > 0 && bytesDelta >= _opt.SnapshotEveryNBytes;
            return timeOk || opsOk || bytesOk;
        }

        private bool ShouldEnterLatencyThrottle()
        {
            if (_opt.LatencySloMilliseconds <= 0)
                return false;

            if (_journal.RecentAppendLatencyMs <= _opt.LatencySloMilliseconds)
                return false;

            _latencyThrottleTimestamp = _timeProvider.GetTimestamp();
            return true;
        }
    }
}
