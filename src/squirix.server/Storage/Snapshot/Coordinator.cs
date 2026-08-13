using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>
/// Coordinates safe snapshot creation.
/// Background snapshots respect interval, volume, and memory-pressure throttles.
/// Concurrency: ensures at most one snapshot runs at a time using an interlocked flag.
/// Ordering guarantee vs writes: before taking a snapshot we flush the journal and record seqAtFlush = journal.NextSequence - 1.
/// The snapshot reflects all effects of operations with Seq less or equal to seqAtFlush. Recovery will replay only operations with Seq > seqAtFlush.
/// Snapshot cut is two-phase: a brief barrier captures a consistent in-memory view under the journal mutation gate, then serialization and
/// manifest I/O run outside the gate so large snapshots do not stop-the-world block durable memory applies.
/// <see cref="SnapshotCompleted" /> is raised only after the journal mutation gate is released so subscribers can safely run maintenance (for example journal compaction) that
/// re-enters the
/// writer.
/// </summary>
internal sealed class Coordinator
{
    private readonly IBackgroundSnapshotMemoryThrottle _backgroundSnapshotMemoryThrottle;
    private readonly CaptureScratch _captureScratch = new();
    private readonly ISnapshotEntryCapture _entryCapture;
    private readonly IIdempotencySnapshotExporter _idempotency;
    private readonly Ledger _manifestStore;
    private readonly string _nodeId;
    private readonly ISnapshotWriter _snapWriter;
    private readonly ISnapshotTelemetry _telemetry;
    private readonly TriggerState _triggerState;
    private int _snapshotInFlight;

    internal Coordinator(TriggerOptions opt, IJournalMetrics journal, CoordinatorDependencies deps)
    {
        _triggerState = new TriggerState(opt, journal);
        ArgumentNullException.ThrowIfNull(deps);
        _entryCapture = deps.EntryCapture;
        _snapWriter = deps.SnapWriter;
        _manifestStore = deps.Ledger;
        _idempotency = deps.Idempotency;
        _nodeId = deps.NodeId;
        _backgroundSnapshotMemoryThrottle = deps.BackgroundSnapshotMemoryThrottle;
        _telemetry = deps.Telemetry;
    }

    public event EventHandler<CompletedEventArgs>? SnapshotCompleted;

    internal bool IsInFlight => Volatile.Read(ref _snapshotInFlight) is not 0;

    internal async ValueTask TrySnapshotAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        if (!_triggerState.ShouldTrigger(DateTime.UtcNow, IsInFlight))
            return;
        if (ShouldSuppressBackgroundSnapshot())
            return;

        if (Interlocked.CompareExchange(ref _snapshotInFlight, 1, 0) is not 0)
            return;

        using var activity = _telemetry.BeginCreate();
        var started = Stopwatch.GetTimestamp();
        var result = "failure";
        try
        {
            var snapshotRef = await journal.ExecuteSnapshotCutAsync(
                (Coordinator: this, Activity: activity, Journal: journal),
                static async (state, _, ct) =>
                {
                    var captured = await state.Coordinator.CaptureSnapshotBundleAsync(state.Journal, ct).ConfigureAwait(false);
                    state.Activity?.SetTag("snapshot.items_count", InvariantDigitStrings.Format(captured.Items.Count));
                    return captured;
                },
                static (state, seqAtFlush, captured, ct) => state.Coordinator.PublishSnapshotAsync(seqAtFlush, captured, state.Activity, ct),
                cancellationToken).ConfigureAwait(false);

            SnapshotCompleted?.Invoke(this, new CompletedEventArgs(snapshotRef));

            result = "success";
            _triggerState.ClearLatencyThrottle();
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(started);
            _telemetry.RecordDuration(_nodeId, result, elapsed);

            activity?.SetTag("snapshot.result", result);
            activity?.SetTag("snapshot.duration_ms", InvariantDigitStrings.Format(elapsed.TotalMilliseconds));

            Volatile.Write(ref _snapshotInFlight, 0);
        }
    }

    private async ValueTask<CapturedSnapshotBundle> CaptureSnapshotBundleAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        _captureScratch.Clear();
        var utcNow = DateTime.UtcNow;
        await _entryCapture.CaptureEntriesAsync(_captureScratch.Items, utcNow, cancellationToken).ConfigureAwait(false);

        _idempotency.ExportSnapshot(_captureScratch.IdempotencyRecords, utcNow);
        return new CapturedSnapshotBundle(_captureScratch.Items, journal.CurrentSegmentIndex, journal.NextSequence, _captureScratch.IdempotencyRecords);
    }

    private async ValueTask<SnapshotRef> PublishSnapshotAsync(
        ulong seqAtFlush,
        CapturedSnapshotBundle captured,
        ISnapshotTraceScope? currentActivity,
        CancellationToken cancellationToken)
    {
        currentActivity?.SetTag("snapshot.seq_at_flush", InvariantDigitStrings.Format(seqAtFlush));

        var prev = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var nextIndex = (prev.LastSnapshot?.Index ?? 0) + 1;
        currentActivity?.SetTag("snapshot.index", InvariantDigitStrings.Format(nextIndex));

        var path = await _snapWriter.WriteAsync(nextIndex, captured.Items, captured.IdempotencyRecordsAtFlush, cancellationToken).ConfigureAwait(false);
        currentActivity?.SetTag("snapshot.path", path);

        var now = DateTime.UtcNow;
        var updated = new State
        {
            Format = prev.Format,
            CurrentJournal = prev.CurrentJournal,
            NextSequence = captured.NextSequenceAtFlush,
            LastSnapshot = new SnapshotRef
            {
                Index = nextIndex,
                Path = path,
                CreatedUtc = now,
                LastAppliedSequence = seqAtFlush,
                ReplayFromJournalSegment = captured.ReplayFromJournalSegmentAtFlush,
            },
        };
        await _manifestStore.WriteAsync(updated, cancellationToken).ConfigureAwait(false);

        _triggerState.RecordSuccess(now);
        return updated.LastSnapshot;
    }

    private bool ShouldSuppressBackgroundSnapshot() => _backgroundSnapshotMemoryThrottle.ShouldSuppressBackgroundSnapshot();

    private sealed record CapturedSnapshotBundle(
        List<(CacheKey Key, NodeCacheEntry<object?> Entry)> Items,
        int ReplayFromJournalSegmentAtFlush,
        ulong NextSequenceAtFlush,
        IReadOnlyList<PersistedIdempotencyRecord> IdempotencyRecordsAtFlush);

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
    /// </summary>
    private sealed class TriggerState
    {
        private readonly IJournalMetrics _journal;
        private readonly TriggerOptions _opt;
        private long _bytesAtLast;
        private DateTime _lastSnapshotUtc = DateTime.MinValue;
        private DateTime _latencyThrottledUntilUtc = DateTime.MinValue;
        private long _opsAtLast;

        internal TriggerState(TriggerOptions opt, IJournalMetrics journal)
        {
            _opt = opt;
            _journal = journal;
        }

        /// <summary>Resets the latency throttle so the next evaluation may proceed normally.</summary>
        internal void ClearLatencyThrottle() => _latencyThrottledUntilUtc = DateTime.MinValue;

        /// <summary>Records the journal baseline after a successful snapshot.</summary>
        /// <param name="now">UTC time of the completed snapshot.</param>
        internal void RecordSuccess(DateTime now)
        {
            _lastSnapshotUtc = now;
            _opsAtLast = _journal.AppendedOps;
            _bytesAtLast = _journal.AppendedBytes;
        }

        /// <summary>
        /// Returns <see langword="true" /> when conditions are met to start a new snapshot.
        /// </summary>
        /// <param name="utcNow">Current UTC time used for all-time comparisons.</param>
        /// <param name="isInFlight">Whether a snapshot is already running on the coordinator.</param>
        /// <returns><see langword="true" /> if a snapshot should be triggered; otherwise <see langword="false" />.</returns>
        internal bool ShouldTrigger(DateTime utcNow, bool isInFlight)
        {
            if (IsBlockedFromTriggering(utcNow, isInFlight))
                return false;

            var opsDelta = _journal.AppendedOps - _opsAtLast;
            var bytesDelta = _journal.AppendedBytes - _bytesAtLast;
            if (_opt.JournalGrowthThrottleBytes > 0 && bytesDelta < _opt.JournalGrowthThrottleBytes)
                return false;

            return MeetsAnyTriggerThreshold(utcNow, opsDelta, bytesDelta);
        }

        private bool IsBlockedFromTriggering(DateTime utcNow, bool isInFlight)
        {
            if (_latencyThrottledUntilUtc > utcNow)
                return true;

            if (ShouldEnterLatencyThrottle(utcNow))
                return true;

            if (_lastSnapshotUtc != DateTime.MinValue && utcNow - _lastSnapshotUtc < _opt.MinGapBetweenSnapshots)
                return true;

            return isInFlight;
        }

        private bool MeetsAnyTriggerThreshold(DateTime utcNow, long opsDelta, long bytesDelta)
        {
            var anyActivity = opsDelta > 0 || bytesDelta > 0;
            var timeOk = _opt.SnapshotInterval > TimeSpan.Zero && (_lastSnapshotUtc == DateTime.MinValue || utcNow - _lastSnapshotUtc >= _opt.SnapshotInterval) && anyActivity;
            var opsOk = _opt.SnapshotEveryNOps > 0 && opsDelta >= _opt.SnapshotEveryNOps;
            var bytesOk = _opt.SnapshotEveryNBytes > 0 && bytesDelta >= _opt.SnapshotEveryNBytes;
            return timeOk || opsOk || bytesOk;
        }

        private bool ShouldEnterLatencyThrottle(DateTime utcNow)
        {
            if (_opt.LatencySloMilliseconds <= 0)
                return false;

            if (_journal.RecentAppendLatencyMs <= _opt.LatencySloMilliseconds)
                return false;

            var backoff = _opt.LatencyThrottleDuration <= TimeSpan.Zero ? TimeSpan.FromSeconds(10) : _opt.LatencyThrottleDuration;
            _latencyThrottledUntilUtc = utcNow + backoff;
            return true;
        }
    }
}
