using System;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Endpoint;

/// <summary>
/// Builds health-ready diagnostics for REST endpoints.
/// </summary>
internal sealed class HealthReadyDetailsProvider : IHealthReadyDetailsProvider
{
    private readonly ClusterConfig _cluster;
    private readonly IJournalCompactionStatus _compaction;
    private readonly IJournalCoordinator _journal;
    private readonly ManifestStore _manifestStore;
    private readonly IMemoryUsageAccounting _memoryAccounting;
    private readonly IMemoryPressureStateEvaluator _memoryEvaluator;
    private readonly IOptions<MemoryPressureOptions> _memoryPressureOptions;
    private readonly SnapshotCoordinator<object?> _snapshot;

    public HealthReadyDetailsProvider(
        ManifestStore manifestStore,
        IJournalCoordinator journal,
        SnapshotCoordinator<object?> snapshot,
        IJournalCompactionStatus compaction,
        ClusterConfig cluster,
        IMemoryUsageAccounting memoryAccounting,
        IMemoryPressureStateEvaluator memoryEvaluator,
        IOptions<MemoryPressureOptions> memoryPressureOptions)
    {
        _manifestStore = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _snapshot = snapshot ?? throw new ArgumentNullException(nameof(snapshot));
        _compaction = compaction ?? throw new ArgumentNullException(nameof(compaction));
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _memoryAccounting = memoryAccounting ?? throw new ArgumentNullException(nameof(memoryAccounting));
        _memoryEvaluator = memoryEvaluator ?? throw new ArgumentNullException(nameof(memoryEvaluator));
        _memoryPressureOptions = memoryPressureOptions ?? throw new ArgumentNullException(nameof(memoryPressureOptions));
    }

    /// <inheritdoc />
    public HealthReadyDetailsSnapshot GetSnapshot()
    {
        var manifest = _manifestStore.ReadCurrentOrDefault();
        var lastApplied = manifest.LastSnapshot?.LastAppliedSequence ?? 0UL;
        var nextSeq = _journal.NextSequence;

        long journalBacklogOps = 0;
        if (nextSeq > lastApplied)
            journalBacklogOps = (long)(nextSeq - lastApplied);

        double? snapshotAgeSeconds = null;
        if (manifest.LastSnapshot?.Path != null)
        {
            try
            {
                snapshotAgeSeconds = Math.Max(0, (DateTime.UtcNow - manifest.LastSnapshot.CreatedUtc).TotalSeconds);
            }
            catch
            {
                snapshotAgeSeconds = null;
            }
        }

        var compactionState = _compaction.State switch
        {
            CompactionState.Idle => "Idle",
            CompactionState.Waiting => "Waiting",
            CompactionState.Running => "Running",
            CompactionState.BackingOff => "BackingOff",
            CompactionState.Failed => "Failed",
            _ => throw new ArgumentOutOfRangeException(nameof(_compaction.State), _compaction.State, "Unsupported compaction state."),
        };
        var compaction = new HealthCompactionSnapshot(compactionState, _compaction.LastRunUtc, _compaction.IsInFlight);
        var clientPool = new HealthClientPoolSnapshot(true, _cluster.Peers.Length);
        var coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0));

        var memOpts = _memoryPressureOptions.Value;
        var estimatedBytes = _memoryAccounting.EstimatedBytes;
        var state = _memoryEvaluator.Evaluate(estimatedBytes);
        var pressureStateName = state switch
        {
            MemoryPressureState.Normal => "normal",
            MemoryPressureState.High => "high",
            MemoryPressureState.Critical => "critical",
            _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unsupported memory pressure state."),
        };

        var writeRejectionActive = memOpts is { Enabled: true, MaxEstimatedCacheBytes: > 0, RejectWritesOnCriticalPressure: true };
        var memoryPressure = new HealthMemoryPressureSnapshot(
            pressureStateName,
            memOpts.MaxEstimatedCacheBytes,
            estimatedBytes,
            _memoryAccounting.EntryCount,
            _memoryAccounting.RejectedWriteCount,
            writeRejectionActive);

        return new HealthReadyDetailsSnapshot
        {
            JournalBacklogOps = journalBacklogOps,
            SnapshotAgeSeconds = snapshotAgeSeconds,
            SnapshotInFlight = _snapshot.IsInFlight,
            Compaction = compaction,
            ClientPool = clientPool,
            Coordination = coordination,
            MemoryPressure = memoryPressure,
        };
    }
}
