using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Storage.Snapshot;

/// <summary>
/// Coordinates safe snapshot creation.
/// Background snapshots respect interval, volume, and memory-pressure throttles.
/// Concurrency: ensures at most one snapshot runs at a time using an interlocked flag.
/// Ordering guarantee vs writes: before taking a snapshot we flush the journal and record seqAtFlush = journal.NextSequence - 1.
/// The snapshot reflects all effects of operations with Seq less or equal to seqAtFlush. Recovery will replay only operations with Seq > seqAtFlush.
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
    private readonly IdempotencyStore _idempotency;
    private readonly IJournalMetrics _journal;
    private readonly ManifestStore _manifestStore;
    private readonly IMemoryPressureStateEvaluator _memoryPressureEvaluator;
    private readonly IMemoryUsageAccounting _memoryUsageAccounting;
    private readonly string _nodeId;
    private readonly SnapshotTriggerOptions _opt;
    private readonly ISnapshotWriter _snapWriter;
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
        IdempotencyStore idempotency,
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

    public event Action<Manifest.SnapshotRef>? SnapshotCompleted;

    public bool IsInFlight => Volatile.Read(ref _snapshotInFlight) != 0;

    public async ValueTask TrySnapshotAsync(IJournalCoordinator journal, CancellationToken cancellationToken)
    {
        if (!_opt.Enabled)
            return;

        if (!ShouldTrigger(DateTime.UtcNow))
            return;
        if (ShouldSuppressBackgroundSnapshotDueToCriticalMemoryPressure())
            return;

        if (Interlocked.CompareExchange(ref _snapshotInFlight, 1, 0) != 0)
            return;

        using var activity = ActivitySourceHolder.StartInternal("snapshot.create");
        var sw = Stopwatch.StartNew();
        var result = "failure";
        try
        {
            var snapshotRef = await journal.ExecuteSnapshotCutAsync(
                (Coordinator: this, Activity: activity, Journal: journal),
                static (state, seqAtFlush, ct) => state.Coordinator.ExecuteSnapshotCutAsync(seqAtFlush, state.Activity, state.Journal, ct),
                cancellationToken).ConfigureAwait(false);

            SnapshotCompleted?.Invoke(snapshotRef);

            result = "success";
            _latencyThrottledUntilUtc = DateTime.MinValue;
        }
        finally
        {
            sw.Stop();
            try
            {
                SnapshotMetrics.DurationSeconds.WithLabels(_nodeId, result).Observe(sw.Elapsed.TotalSeconds);
            }
            catch (InvalidOperationException)
            {
                // Metrics emission is best-effort and must not affect snapshot completion.
            }

            _ = activity?.SetTag("snapshot.result", result);
            _ = activity?.SetTag("snapshot.duration_ms", (long)sw.Elapsed.TotalMilliseconds);

            Volatile.Write(ref _snapshotInFlight, 0);
        }
    }

    private async ValueTask<Manifest.SnapshotRef> ExecuteSnapshotCutAsync(
        ulong seqAtFlush,
        Activity? currentActivity,
        IJournalCoordinator currentJournal,
        CancellationToken cancellationToken)
    {
        _ = currentActivity?.SetTag("snapshot.seq_at_flush", (long)seqAtFlush);

        var items = new List<(CacheKey Key, object Json)>();
        await foreach (var (key, entry) in _cache.EnumerateLiveAsync(cancellationToken))
        {
            if (entry.ExpiresUtc is { } exp && exp <= DateTime.UtcNow)
                continue;

            var payload = DiscriminatedEntryJsonWriter.BuildEntryJson(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags);
            using var doc = JsonDocument.Parse(payload);
            items.Add((key, doc.RootElement.Clone()));
        }

        _ = currentActivity?.SetTag("snapshot.items_count", items.Count);

        var prev = _manifestStore.ReadCurrentOrDefault();
        var nextIndex = (prev.LastSnapshot?.Index ?? 0) + 1;
        _ = currentActivity?.SetTag("snapshot.index", nextIndex);

        var idempotencyRecords = _idempotency.ExportSnapshot(DateTime.UtcNow);
        var path = await _snapWriter.WriteAsync(nextIndex, items, idempotencyRecords, cancellationToken).ConfigureAwait(false);
        _ = currentActivity?.SetTag("snapshot.path", path);

        var now = DateTime.UtcNow;
        var updated = new Manifest
        {
            Format = prev.Format,
            CurrentJournal = prev.CurrentJournal,
            NextSequence = currentJournal.NextSequence,
            LastSnapshot = new Manifest.SnapshotRef
            {
                Index = nextIndex,
                Path = path,
                CreatedUtc = now,
                LastAppliedSequence = seqAtFlush,
                ReplayFromJournalSegment = currentJournal.CurrentSegmentIndex,
            },
        };
        _manifestStore.Write(updated);

        _lastSnapshotUtc = now;
        _opsAtLast = _journal.AppendedOps;
        _bytesAtLast = _journal.AppendedBytes;
        return updated.LastSnapshot;
    }

    private bool ShouldSuppressBackgroundSnapshotDueToCriticalMemoryPressure() =>
        _memoryPressureEvaluator.Evaluate(_memoryUsageAccounting.EstimatedBytes) == MemoryPressureState.Critical;

    private bool ShouldTrigger(DateTime utcNow)
    {
        if (!_opt.Enabled)
            return false;
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
}
