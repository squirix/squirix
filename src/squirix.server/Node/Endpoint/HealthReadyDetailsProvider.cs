using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Builds health-ready diagnostics for REST endpoints.</summary>
internal sealed class HealthReadyDetailsProvider : IHealthReadyDetailsProvider
{
    private readonly ClusterConfig _cluster;
    private readonly IJournalCompactionStatus _compaction;
    private readonly IJournalCoordinator _journal;
    private readonly ManifestStore _manifestStore;
    private readonly IMemoryUsageAccounting _memoryAccounting;
    private readonly IMemoryPressureStateEvaluator _memoryEvaluator;
    private readonly MemoryPressureOptions _memoryPressureOptions;
    private readonly IRetentionCleanupReadinessStatus _retentionCleanup;
    private readonly SnapshotCoordinator<object?> _snapshot;

    public HealthReadyDetailsProvider(
        ManifestStore manifestStore,
        IRetentionCleanupReadinessStatus retentionCleanup,
        IJournalCoordinator journal,
        SnapshotCoordinator<object?> snapshot,
        IJournalCompactionStatus compaction,
        ClusterConfig cluster,
        IMemoryUsageAccounting memoryAccounting,
        IMemoryPressureStateEvaluator memoryEvaluator,
        MemoryPressureOptions memoryPressureOptions)
    {
        _manifestStore = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        _retentionCleanup = retentionCleanup ?? throw new ArgumentNullException(nameof(retentionCleanup));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        _compaction = compaction ?? throw new ArgumentNullException(nameof(compaction));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _memoryAccounting = memoryAccounting ?? throw new ArgumentNullException(nameof(memoryAccounting));
        _memoryEvaluator = memoryEvaluator ?? throw new ArgumentNullException(nameof(memoryEvaluator));
        _memoryPressureOptions = memoryPressureOptions ?? throw new ArgumentNullException(nameof(memoryPressureOptions));
    }

    /// <inheritdoc />
    public async Task<HealthReadyDetailsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
    {
        var manifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var lastApplied = manifest.LastSnapshot?.LastAppliedSequence ?? 0UL;
        var nextSeq = _journal.NextSequence;

        ulong journalBacklogOps = 0;
        if (nextSeq > lastApplied)
            journalBacklogOps = nextSeq - lastApplied;

        double? snapshotAgeSeconds = null;
        if (manifest.LastSnapshot?.Path is not null)
        {
            snapshotAgeSeconds = Math.Max(0, (DateTime.UtcNow - manifest.LastSnapshot.CreatedUtc).TotalSeconds);
        }

        var compactionState = _compaction.State switch
        {
            CompactionState.Idle => "Idle",
            CompactionState.Waiting => "Waiting",
            CompactionState.Running => "Running",
            CompactionState.BackingOff => "BackingOff",
            CompactionState.Failed => "Failed",
            _ => throw new InvalidOperationException($"Unsupported compaction state: {_compaction.State}."),
        };
        var compaction = new HealthCompactionSnapshot(compactionState, _compaction.LastRunUtc, _compaction.IsInFlight);
        var clientPool = new HealthClientPoolSnapshot(true, _cluster.Peers.Length);
        var coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0));

        var memoryPressure = BuildMemoryPressureSnapshot();
        var retentionCleanup = BuildRetentionCleanupSnapshot();

        return new HealthReadyDetailsSnapshot
        {
            JournalBacklogOps = journalBacklogOps,
            SnapshotAgeSeconds = snapshotAgeSeconds,
            SnapshotInFlight = _snapshot.IsInFlight,
            Compaction = compaction,
            ClientPool = clientPool,
            Coordination = coordination,
            MemoryPressure = memoryPressure,
            RetentionCleanup = retentionCleanup,
        };
    }

    private HealthMemoryPressureSnapshot BuildMemoryPressureSnapshot()
    {
        var estimatedBytes = _memoryAccounting.EstimatedBytes;
        var state = _memoryEvaluator.Evaluate(estimatedBytes);
        var pressureStateName = state switch
        {
            MemoryPressureState.Normal => "normal",
            MemoryPressureState.High => "high",
            MemoryPressureState.Critical => "critical",
            _ => throw new InvalidOperationException($"Unsupported memory pressure state: {state}."),
        };

        return new HealthMemoryPressureSnapshot(
            pressureStateName,
            _memoryPressureOptions.MaxEstimatedCacheBytes,
            estimatedBytes,
            _memoryAccounting.EntryCount,
            _memoryAccounting.RejectedWriteCount,
            true);
    }

    private HealthRetentionCleanupSnapshot BuildRetentionCleanupSnapshot() =>
        new(
            _retentionCleanup.IsDegraded,
            _retentionCleanup.ConsecutiveWriteFailures,
            _retentionCleanup.RecentFailureCount,
            _retentionCleanup.LastFailureUtc);
}
