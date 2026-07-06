using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;

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
/// <typeparam name="T">
/// The value type stored in the cache being snapshot (e.g., <c>object?</c> for untyped payloads
/// or a concrete DTO). All nodes for this cache should use the same <typeparamref name="T" />.
/// </typeparam>
internal sealed class SnapshotCoordinator<T>
{
    private readonly ILocalCacheSnapshotReader<T> _cache;
    private readonly RpcMutationIdempotencyStore _idempotency;
    private readonly IJournalMetrics _journal;
    private readonly ManifestStore _manifestStore;
    private readonly IMemoryPressureStateEvaluator _memoryPressureEvaluator;
    private readonly IMemoryUsageAccounting _memoryUsageAccounting;
    private readonly string _nodeId;
    private readonly SnapshotTriggerOptions _opt;
    private readonly ISnapshotWriter _snapWriter;
    private readonly List<(CacheKey Key, CacheEntry<object?> Entry)> _captureItems = [];
    private readonly List<PersistedIdempotencyRecord> _captureIdempotency = [];
    private long _bytesAtLast;
    private DateTime _lastSnapshotUtc = DateTime.MinValue;
    private DateTime _latencyThrottledUntilUtc = DateTime.MinValue;
    private long _opsAtLast;
    private int _snapshotInFlight;

    public SnapshotCoordinator(
        SnapshotTriggerOptions opt,
        IJournalMetrics journal,
        ILocalCacheSnapshotReader<T> cache,
        ISnapshotWriter snapWriter,
        ManifestStore manifestStore,
        RpcMutationIdempotencyStore idempotency,
        ClusterConfig cluster,
        IMemoryPressureStateEvaluator memoryPressureEvaluator,
        IMemoryUsageAccounting memoryUsageAccounting)
    {
        _opt = opt;
        _journal = journal;
        _cache = cache;
        _snapWriter = snapWriter;
        _manifestStore = manifestStore;
        _idempotency = idempotency;
        _nodeId = cluster.NodeId;
        _memoryPressureEvaluator = memoryPressureEvaluator ?? throw new ArgumentNullException(nameof(memoryPressureEvaluator));
        _memoryUsageAccounting = memoryUsageAccounting ?? throw new ArgumentNullException(nameof(memoryUsageAccounting));
    }

    public event EventHandler<SnapshotCompletedEventArgs>? SnapshotCompleted;

    public bool IsInFlight => Volatile.Read(ref _snapshotInFlight) is not 0;

    public async ValueTask TrySnapshotAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        if (!ShouldTrigger(DateTime.UtcNow))
            return;
        if (ShouldSuppressBackgroundSnapshotDueToCriticalMemoryPressure())
            return;

        if (Interlocked.CompareExchange(ref _snapshotInFlight, 1, 0) is not 0)
            return;

        using var activity = ActivitySourceHolder.StartInternal("snapshot.create");
        var started = Stopwatch.GetTimestamp();
        var result = "failure";
        try
        {
            var snapshotRef = await journal.ExecuteSnapshotCutAsync(
                (Coordinator: this, Activity: activity, Journal: journal),
                static async (state, __, ct) =>
                {
                    var captured = await state.Coordinator.CaptureSnapshotBundleAsync(state.Journal, ct).ConfigureAwait(false);
                    _ = state.Activity?.SetTag("snapshot.items_count", captured.Items.Count);
                    return captured;
                },
                static (state, seqAtFlush, captured, ct) => state.Coordinator.PublishSnapshotAsync(seqAtFlush, captured, state.Activity, ct),
                cancellationToken).ConfigureAwait(false);

            SnapshotCompleted?.Invoke(this, new SnapshotCompletedEventArgs(snapshotRef));

            result = "success";
            _latencyThrottledUntilUtc = DateTime.MinValue;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(started);
            SnapshotMetrics.DurationSeconds.WithLabels(_nodeId, result).Observe(elapsed.TotalSeconds);

            _ = activity?.SetTag("snapshot.result", result);
            _ = activity?.SetTag("snapshot.duration_ms", elapsed.TotalMilliseconds);

            Volatile.Write(ref _snapshotInFlight, 0);
        }
    }

    private async ValueTask<CapturedSnapshotBundle> CaptureSnapshotBundleAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        _captureItems.Clear();
        var utcNow = DateTime.UtcNow;
        var capacity = _cache is ILocalCacheStats stats ? stats.EntryCount : 0;
        if (_captureItems.Capacity < capacity)
            _captureItems.Capacity = capacity;

        await foreach (var (key, entry) in _cache.EnumerateLiveAsync(cancellationToken).ConfigureAwait(false))
        {
            if (entry.ExpiresUtc is { } exp && exp <= utcNow)
                continue;

            _captureItems.Add((key, ToSnapshotEntry(entry)));
        }

        _idempotency.ExportSnapshot(_captureIdempotency, utcNow);
        return new CapturedSnapshotBundle(_captureItems, journal.CurrentSegmentIndex, journal.NextSequence, _captureIdempotency);

        static CacheEntry<object?> ToSnapshotEntry(CacheEntry<T> source)
        {
            // Serialize an arbitrary object to its JsonElement form once here, so the snapshot encoder's
            // repeated length/write passes never re-serialize it. Directly-encodable values pass through
            // unchanged, preserving the zero-copy fast path for the object pipeline.
            var normalized = CacheEntryCodec.NormalizeValue(source.Value);
            if (source is CacheEntry<object?> objectEntry && ReferenceEquals(normalized, source.Value))
                return objectEntry;

            return new CacheEntry<object?>
            {
                Value = normalized,
                ExpiresUtc = source.ExpiresUtc,
                Expiration = source.Expiration,
                Version = source.Version,
                Tags = source.Tags,
            };
        }
    }

    private async ValueTask<ManifestState.SnapshotRef> PublishSnapshotAsync(
        ulong seqAtFlush,
        CapturedSnapshotBundle captured,
        Activity? currentActivity,
        CancellationToken cancellationToken)
    {
        _ = currentActivity?.SetTag("snapshot.seq_at_flush", seqAtFlush);

        var prev = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var nextIndex = (prev.LastSnapshot?.Index ?? 0) + 1;
        _ = currentActivity?.SetTag("snapshot.index", nextIndex);

        var path = await _snapWriter.WriteAsync(nextIndex, captured.Items, captured.IdempotencyRecordsAtFlush, cancellationToken).ConfigureAwait(false);
        _ = currentActivity?.SetTag("snapshot.path", path);

        var now = DateTime.UtcNow;
        var updated = new ManifestState
        {
            Format = prev.Format,
            CurrentJournal = prev.CurrentJournal,
            NextSequence = captured.NextSequenceAtFlush,
            LastSnapshot = new ManifestState.SnapshotRef
            {
                Index = nextIndex,
                Path = path,
                CreatedUtc = now,
                LastAppliedSequence = seqAtFlush,
                ReplayFromJournalSegment = captured.ReplayFromJournalSegmentAtFlush,
            },
        };
        await _manifestStore.WriteAsync(updated, cancellationToken).ConfigureAwait(false);

        _lastSnapshotUtc = now;
        _opsAtLast = _journal.AppendedOps;
        _bytesAtLast = _journal.AppendedBytes;
        return updated.LastSnapshot;
    }

    private bool ShouldSuppressBackgroundSnapshotDueToCriticalMemoryPressure() =>
        _memoryPressureEvaluator.Evaluate(_memoryUsageAccounting.EstimatedBytes) is MemoryPressureState.Critical;

    private bool ShouldTrigger(DateTime utcNow)
    {
        if (_latencyThrottledUntilUtc > utcNow)
            return false;

        if (_opt.LatencySloMilliseconds > 0)
        {
            var observedLatency = _journal.RecentAppendLatencyMs;
            if (observedLatency > _opt.LatencySloMilliseconds)
            {
                var backoff = _opt.LatencyThrottleDuration <= TimeSpan.Zero ? TimeSpan.FromSeconds(10) : _opt.LatencyThrottleDuration;
                _latencyThrottledUntilUtc = utcNow + backoff;
                return false;
            }
        }

        if (_lastSnapshotUtc != DateTime.MinValue && utcNow - _lastSnapshotUtc < _opt.MinGapBetweenSnapshots)
            return false;
        if (IsInFlight)
            return false;

        var opsDelta = _journal.AppendedOps - _opsAtLast;
        var bytesDelta = _journal.AppendedBytes - _bytesAtLast;
        if (_opt.JournalGrowthThrottleBytes > 0 && bytesDelta < _opt.JournalGrowthThrottleBytes)
            return false;

        var anyActivity = opsDelta > 0 || bytesDelta > 0;
        var timeOk = _opt.SnapshotInterval > TimeSpan.Zero && (_lastSnapshotUtc == DateTime.MinValue || utcNow - _lastSnapshotUtc >= _opt.SnapshotInterval) && anyActivity;
        var opsOk = _opt.SnapshotEveryNOps > 0 && opsDelta >= _opt.SnapshotEveryNOps;
        var bytesOk = _opt.SnapshotEveryNBytes > 0 && bytesDelta >= _opt.SnapshotEveryNBytes;
        return timeOk || opsOk || bytesOk;
    }

    private sealed record CapturedSnapshotBundle(
        List<(CacheKey Key, CacheEntry<object?> Entry)> Items,
        int ReplayFromJournalSegmentAtFlush,
        ulong NextSequenceAtFlush,
        IReadOnlyList<PersistedIdempotencyRecord> IdempotencyRecordsAtFlush);
}
