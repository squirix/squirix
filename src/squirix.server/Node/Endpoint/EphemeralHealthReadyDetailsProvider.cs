using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Endpoint;

/// <summary>
/// Builds health-ready diagnostics when persistence is disabled.
/// </summary>
internal sealed class EphemeralHealthReadyDetailsProvider : IHealthReadyDetailsProvider
{
    private readonly ClusterConfig _cluster;
    private readonly IMemoryUsageAccounting _memoryAccounting;
    private readonly IMemoryPressureStateEvaluator _memoryEvaluator;
    private readonly MemoryPressureOptions _memoryPressureOptions;

    public EphemeralHealthReadyDetailsProvider(
        ClusterConfig cluster,
        IMemoryUsageAccounting memoryAccounting,
        IMemoryPressureStateEvaluator memoryEvaluator,
        MemoryPressureOptions memoryPressureOptions)
    {
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _memoryAccounting = memoryAccounting ?? throw new ArgumentNullException(nameof(memoryAccounting));
        _memoryEvaluator = memoryEvaluator ?? throw new ArgumentNullException(nameof(memoryEvaluator));
        _memoryPressureOptions = memoryPressureOptions ?? throw new ArgumentNullException(nameof(memoryPressureOptions));
    }

    /// <inheritdoc />
    public HealthReadyDetailsSnapshot GetSnapshot()
    {
        var compaction = new HealthCompactionSnapshot("Idle", null, false);
        var clientPool = new HealthClientPoolSnapshot(true, _cluster.Peers.Length);
        var coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0));

        var estimatedBytes = _memoryAccounting.EstimatedBytes;
        var state = _memoryEvaluator.Evaluate(estimatedBytes);
        var pressureStateName = state switch
        {
            MemoryPressureState.Normal => "normal",
            MemoryPressureState.High => "high",
            MemoryPressureState.Critical => "critical",
            _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unsupported memory pressure state."),
        };

        var memoryPressure = new HealthMemoryPressureSnapshot(
            pressureStateName,
            _memoryPressureOptions.MaxEstimatedCacheBytes,
            estimatedBytes,
            _memoryAccounting.EntryCount,
            _memoryAccounting.RejectedWriteCount,
            true);

        return new HealthReadyDetailsSnapshot
        {
            JournalBacklogOps = 0,
            SnapshotAgeSeconds = null,
            SnapshotInFlight = false,
            Compaction = compaction,
            ClientPool = clientPool,
            Coordination = coordination,
            MemoryPressure = memoryPressure,
        };
    }
}
