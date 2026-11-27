using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Diagnostics;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Node-owned endpoint execution services consumed by transport adapters through runtime contracts.</summary>
internal static class NodeEndpointServiceRegistration
{
    extension(NodeServices services)
    {
        /// <summary>Registers inbound endpoint cache routing used by gRPC adapters.</summary>
        /// <param name="persistenceEnabled">When true, registers durable health-ready detail providers.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal NodeServices AddSquirixNodeEndpointServices(bool persistenceEnabled = false)
        {
            _ = services.Own<IInboundEndpointCacheOperations<StoredValue>>(static sp =>
                new InboundEndpointCacheOperations<StoredValue>(sp.Require<ILogicalNamespacedCache<StoredValue>>()));
            if (persistenceEnabled)
            {
                _ = services.Own<IHealthReadyDetailsProvider>(static sp => new HealthReadyDetailsProvider(
                    new HealthReadyDependencies(
                        sp.Require<Ledger>(),
                        sp.Require<IRetentionCleanupReadinessStatus>(),
                        sp.Require<IJournalCoordinator>(),
                        sp.Require<Coordinator>(),
                        sp.Require<TopologyOptions>()),
                    FindMemoryPressureReader(sp),
                    sp.Find<IReplicaStatusSource>(),
                    sp.Require<TimeProvider>()));
            }
            else
            {
                _ = services.Own<IHealthReadyDetailsProvider>(static sp => new EphemeralHealthReadyDetailsProvider(
                    sp.Require<TopologyOptions>(),
                    FindMemoryPressureReader(sp)));
            }

            return services;
        }
    }

    private static Func<HealthMemoryPressureSnapshot>? FindMemoryPressureReader(IServiceProvider services)
    {
        if (services.Find<IMemoryUsageAccounting>() is not { } accounting)
            return null;

        var evaluator = services.Require<IMemoryPressureStateEvaluator>();
        var options = services.Require<PressureOptions>();
        return () => BuildMemoryPressureSnapshot(accounting, evaluator, options);
    }

    private static HealthMemoryPressureSnapshot BuildMemoryPressureSnapshot(
        IMemoryUsageAccounting memoryAccounting,
        IMemoryPressureStateEvaluator memoryEvaluator,
        PressureOptions memoryPressureOptions)
    {
        var estimatedBytes = memoryAccounting.ReadEstimatedBytes();
        var state = memoryEvaluator.Evaluate(estimatedBytes);
        var pressureStateName = state switch
        {
            PressureLevel.Normal => "normal",
            PressureLevel.High => "high",
            PressureLevel.Critical => "critical",
            _ => throw new InvalidOperationException("Unsupported memory pressure state."),
        };

        return new HealthMemoryPressureSnapshot(
            pressureStateName,
            memoryPressureOptions.MaxEstimatedCacheBytes,
            estimatedBytes,
            memoryAccounting.ReadEntryCount(),
            memoryAccounting.ReadRejectedWriteCount(),
            true);
    }

    /// <summary>Builds health-ready diagnostics when persistence is disabled.</summary>
    [Immutable]
    private sealed class EphemeralHealthReadyDetailsProvider : IHealthReadyDetailsProvider
    {
        private readonly TopologyOptions _cluster;
        private readonly Func<HealthMemoryPressureSnapshot>? _readMemoryPressure;

        internal EphemeralHealthReadyDetailsProvider(TopologyOptions cluster, Func<HealthMemoryPressureSnapshot>? readMemoryPressure)
        {
            ArgumentNullException.ThrowIfNull(cluster);
            _cluster = cluster;
            _readMemoryPressure = readMemoryPressure;
        }

        /// <inheritdoc />
        public Task<HealthReadyDetailsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
        {
            var clientPool = new HealthClientPoolSnapshot(true, _cluster.Peers.Count);
            var coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0));

            var healthReadyDetailsSnapshot = new HealthReadyDetailsSnapshot
            {
                JournalBacklogOps = 0,
                SnapshotAgeSeconds = null,
                SnapshotInFlight = false,
                ClientPool = clientPool,
                Coordination = coordination,
                MemoryPressure = _readMemoryPressure?.Invoke(),
                JournalDisk = new HealthJournalDiskSnapshot("normal", 0, 0, 0, false),
                RetentionCleanup = new HealthRetentionCleanupSnapshot(false, 0, 0, null),
            };
            return Task.FromResult(healthReadyDetailsSnapshot);
        }
    }

    [Immutable]
    private sealed class HealthReadyDependencies
    {
        internal HealthReadyDependencies(
            Ledger manifestStore,
            IRetentionCleanupReadinessStatus retentionCleanup,
            IJournalCoordinator journal,
            Coordinator snapshot,
            TopologyOptions cluster)
        {
            ArgumentNullException.ThrowIfNull(manifestStore);
            ArgumentNullException.ThrowIfNull(retentionCleanup);
            ArgumentNullException.ThrowIfNull(journal);
            ArgumentNullException.ThrowIfNull(snapshot);
            ArgumentNullException.ThrowIfNull(cluster);
            Ledger = manifestStore;
            RetentionCleanup = retentionCleanup;
            Journal = journal;
            Snapshot = snapshot;
            Cluster = cluster;
        }

        internal TopologyOptions Cluster { get; }

        internal IJournalCoordinator Journal { get; }

        internal Ledger Ledger { get; }

        internal IRetentionCleanupReadinessStatus RetentionCleanup { get; }

        internal Coordinator Snapshot { get; }
    }

    /// <summary>Builds health-ready diagnostics for `/health/ready/details`.</summary>
    [Immutable]
    private sealed class HealthReadyDetailsProvider : IHealthReadyDetailsProvider
    {
        private readonly TopologyOptions _cluster;
        private readonly IJournalCoordinator _journal;
        private readonly Ledger _manifestStore;
        private readonly Func<HealthMemoryPressureSnapshot>? _readMemoryPressure;
        private readonly IReplicaStatusSource? _replication;
        private readonly IRetentionCleanupReadinessStatus _retentionCleanup;
        private readonly Coordinator _snapshot;
        private readonly TimeProvider _timeProvider;

        internal HealthReadyDetailsProvider(
            HealthReadyDependencies deps,
            Func<HealthMemoryPressureSnapshot>? readMemoryPressure,
            IReplicaStatusSource? replication,
            TimeProvider timeProvider)
        {
            _replication = replication;
            ArgumentNullException.ThrowIfNull(deps);
            _manifestStore = deps.Ledger;
            _retentionCleanup = deps.RetentionCleanup;
            _journal = deps.Journal;
            _snapshot = deps.Snapshot;
            _cluster = deps.Cluster;
            ArgumentNullException.ThrowIfNull(timeProvider);
            _readMemoryPressure = readMemoryPressure;
            _timeProvider = timeProvider;
        }

        /// <inheritdoc />
        public async Task<HealthReadyDetailsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
        {
            var manifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var lastApplied = manifest.LastSnapshot?.LastAppliedSequence ?? 0UL;
            var nextSeq = _journal.NextSequence;
            var lastAllocated = nextSeq > 0UL ? nextSeq - 1UL : 0UL;

            ulong journalBacklogOps = 0;
            if (lastAllocated > lastApplied)
                journalBacklogOps = lastAllocated - lastApplied;

            double? snapshotAgeSeconds = null;
            if (manifest.LastSnapshot != null)
                snapshotAgeSeconds = Math.Max(0, (_timeProvider.GetUtcNow().UtcDateTime - manifest.LastSnapshot.CreatedUtc).TotalSeconds);

            var clientPool = new HealthClientPoolSnapshot(true, _cluster.Peers.Count);
            var coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0));

            var memoryPressure = _readMemoryPressure?.Invoke();
            var journalDisk = BuildJournalDiskSnapshot();
            var retentionCleanup = BuildRetentionCleanupSnapshot();
            var replicaGroups = await ReadReplicaGroupsAsync(cancellationToken).ConfigureAwait(false);

            return new HealthReadyDetailsSnapshot
            {
                ReplicaGroups = replicaGroups,
                JournalBacklogOps = journalBacklogOps,
                SnapshotAgeSeconds = snapshotAgeSeconds,
                SnapshotInFlight = _snapshot.IsInFlight,
                ClientPool = clientPool,
                Coordination = coordination,
                MemoryPressure = memoryPressure,
                JournalDisk = journalDisk,
                RetentionCleanup = retentionCleanup,
            };
        }

        /// <summary>Reads the retained size of every served replica group log.</summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>One entry per served group; empty when replication is not configured.</returns>
        private async Task<HealthReplicaGroupSnapshot[]> ReadReplicaGroupsAsync(CancellationToken cancellationToken)
        {
            if (_replication == null)
                return [];

            var groups = await _replication.GetSnapshotsAsync(cancellationToken).ConfigureAwait(false);
            var result = new HealthReplicaGroupSnapshot[groups.Count];
            for (var i = 0; i < result.Length; i++)
                result[i] = new HealthReplicaGroupSnapshot(groups[i].GroupId, groups[i].LogBytes, groups[i].RetainedEntries, groups[i].SnapshotIndex, groups[i].Peers);

            return result;
        }

        private HealthJournalDiskSnapshot BuildJournalDiskSnapshot()
        {
            var usedBytes = _journal.UsedBytes;
            var maxBytes = _journal.MaxBytes;
            var highWaterBytes = _journal.HighWaterBytes;
            var state = JournalSegmentPolicy.EvaluatePressureState(usedBytes, highWaterBytes, maxBytes);
            return new HealthJournalDiskSnapshot(state, maxBytes, usedBytes, highWaterBytes, usedBytes >= maxBytes);
        }

        private HealthRetentionCleanupSnapshot BuildRetentionCleanupSnapshot() => new(
            _retentionCleanup.IsDegraded,
            _retentionCleanup.ConsecutiveWriteFailures,
            _retentionCleanup.RecentFailureCount,
            _retentionCleanup.LastFailureUtc);
    }
}
