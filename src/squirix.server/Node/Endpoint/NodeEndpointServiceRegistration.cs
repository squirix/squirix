using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Diagnostics;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Endpoint;

/// <summary>Node-owned endpoint execution services consumed by transport adapters through runtime contracts.</summary>
internal static class NodeEndpointServiceRegistration
{
    extension(IServiceCollection services)
    {
        /// <summary>Registers inbound endpoint cache routing used by gRPC adapters.</summary>
        /// <param name="persistenceEnabled">When true, registers durable health-ready detail providers.</param>
        /// <returns><paramref name="services" /> for chaining.</returns>
        internal IServiceCollection AddSquirixNodeEndpointServices(bool persistenceEnabled = false)
        {
            _ = services.AddSingleton<IInboundEndpointCacheOperations<object?>, InboundEndpointCacheOperations<object?>>();
            _ = persistenceEnabled ? services.AddSingleton<IHealthReadyDetailsProvider>(static sp => new HealthReadyDetailsProvider(
                new HealthReadyDependencies(
                    sp.GetRequiredService<Ledger>(),
                    sp.GetRequiredService<IRetentionCleanupReadinessStatus>(),
                    sp.GetRequiredService<IJournalCoordinator>(),
                    sp.GetRequiredService<Coordinator>(),
                    sp.GetRequiredService<IJournalCompactionStatus>(),
                    sp.GetRequiredService<TopologyOptions>(),
                    sp.GetRequiredService<IMemoryUsageAccounting>()),
                sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
                sp.GetRequiredService<PressureOptions>())) : services.AddSingleton<IHealthReadyDetailsProvider>(static sp => new EphemeralHealthReadyDetailsProvider(
                sp.GetRequiredService<TopologyOptions>(),
                sp.GetRequiredService<IMemoryUsageAccounting>(),
                sp.GetRequiredService<IMemoryPressureStateEvaluator>(),
                sp.GetRequiredService<PressureOptions>()));

            return services;
        }
    }

    /// <summary>Builds health-ready diagnostics when persistence is disabled.</summary>
    [Immutable]
    private sealed class EphemeralHealthReadyDetailsProvider : IHealthReadyDetailsProvider
    {
        private readonly TopologyOptions _cluster;
        private readonly IMemoryUsageAccounting _memoryAccounting;
        private readonly IMemoryPressureStateEvaluator _memoryEvaluator;
        private readonly PressureOptions _memoryPressureOptions;

        internal EphemeralHealthReadyDetailsProvider(
            TopologyOptions cluster,
            IMemoryUsageAccounting memoryAccounting,
            IMemoryPressureStateEvaluator memoryEvaluator,
            PressureOptions memoryPressureOptions)
        {
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(memoryAccounting);
            ArgumentNullException.ThrowIfNull(memoryEvaluator);
            ArgumentNullException.ThrowIfNull(memoryPressureOptions);
            _cluster = cluster;
            _memoryAccounting = memoryAccounting;
            _memoryEvaluator = memoryEvaluator;
            _memoryPressureOptions = memoryPressureOptions;
        }

        /// <inheritdoc />
        public Task<HealthReadyDetailsSnapshot> GetSnapshotAsync(CancellationToken cancellationToken = default)
        {
            var compaction = new HealthCompactionSnapshot("Idle", null, false);
            var clientPool = new HealthClientPoolSnapshot(true, _cluster.Peers.Length);
            var coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0));

            var estimatedBytes = _memoryAccounting.ReadEstimatedBytes();
            var state = _memoryEvaluator.Evaluate(estimatedBytes);
            var pressureStateName = state switch
            {
                PressureLevel.Normal => "normal",
                PressureLevel.High => "high",
                PressureLevel.Critical => "critical",
                _ => throw new InvalidOperationException("Unsupported memory pressure state."),
            };

            var memoryPressure = new HealthMemoryPressureSnapshot(
                pressureStateName,
                _memoryPressureOptions.MaxEstimatedCacheBytes,
                estimatedBytes,
                _memoryAccounting.ReadEntryCount(),
                _memoryAccounting.ReadRejectedWriteCount(),
                true);

            var healthReadyDetailsSnapshot = new HealthReadyDetailsSnapshot
            {
                JournalBacklogOps = 0,
                SnapshotAgeSeconds = null,
                SnapshotInFlight = false,
                Compaction = compaction,
                ClientPool = clientPool,
                Coordination = coordination,
                MemoryPressure = memoryPressure,
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
            IJournalCompactionStatus compaction,
            TopologyOptions cluster,
            IMemoryUsageAccounting memoryAccounting)
        {
            ArgumentNullException.ThrowIfNull(manifestStore);
            ArgumentNullException.ThrowIfNull(retentionCleanup);
            ArgumentNullException.ThrowIfNull(journal);
            ArgumentNullException.ThrowIfNull(snapshot);
            ArgumentNullException.ThrowIfNull(compaction);
            ArgumentNullException.ThrowIfNull(cluster);
            ArgumentNullException.ThrowIfNull(memoryAccounting);
            Ledger = manifestStore;
            RetentionCleanup = retentionCleanup;
            Journal = journal;
            Snapshot = snapshot;
            Compaction = compaction;
            Cluster = cluster;
            MemoryAccounting = memoryAccounting;
        }

        internal TopologyOptions Cluster { get; }

        internal IJournalCompactionStatus Compaction { get; }

        internal IJournalCoordinator Journal { get; }

        internal Ledger Ledger { get; }

        internal IMemoryUsageAccounting MemoryAccounting { get; }

        internal IRetentionCleanupReadinessStatus RetentionCleanup { get; }

        internal Coordinator Snapshot { get; }
    }

    /// <summary>Builds health-ready diagnostics for `/health/ready/details`.</summary>
    [Immutable]
    private sealed class HealthReadyDetailsProvider : IHealthReadyDetailsProvider
    {
        private readonly TopologyOptions _cluster;
        private readonly IJournalCompactionStatus _compaction;
        private readonly IJournalCoordinator _journal;
        private readonly Ledger _manifestStore;
        private readonly IMemoryUsageAccounting _memoryAccounting;
        private readonly IMemoryPressureStateEvaluator _memoryEvaluator;
        private readonly PressureOptions _memoryPressureOptions;
        private readonly IRetentionCleanupReadinessStatus _retentionCleanup;
        private readonly Coordinator _snapshot;

        internal HealthReadyDetailsProvider(HealthReadyDependencies deps, IMemoryPressureStateEvaluator memoryEvaluator, PressureOptions memoryPressureOptions)
        {
            ArgumentNullException.ThrowIfNull(deps);
            _manifestStore = deps.Ledger;
            _retentionCleanup = deps.RetentionCleanup;
            _journal = deps.Journal;
            _snapshot = deps.Snapshot;
            _compaction = deps.Compaction;
            _cluster = deps.Cluster;
            _memoryAccounting = deps.MemoryAccounting;
            ArgumentNullException.ThrowIfNull(memoryEvaluator);
            ArgumentNullException.ThrowIfNull(memoryPressureOptions);
            _memoryEvaluator = memoryEvaluator;
            _memoryPressureOptions = memoryPressureOptions;
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
            if (manifest.LastSnapshot?.Path != null)
                snapshotAgeSeconds = Math.Max(0, (DateTime.UtcNow - manifest.LastSnapshot.CreatedUtc).TotalSeconds);

            var compactionState = _compaction.State switch
            {
                RunState.Idle => "Idle",
                RunState.Waiting => "Waiting",
                RunState.Running => "Running",
                RunState.BackingOff => "BackingOff",
                RunState.Failed => "Failed",
                _ => throw new InvalidOperationException("Unsupported compaction state."),
            };
            var compaction = new HealthCompactionSnapshot(compactionState, _compaction.LastRunUtc, _compaction.IsInFlight);
            var clientPool = new HealthClientPoolSnapshot(true, _cluster.Peers.Length);
            var coordination = new HealthCoordinationSnapshot(new HealthLeaseSnapshot(false, 0, 0, 0), new HealthWatchSnapshot(false, 0, 0, 0));

            var memoryPressure = BuildMemoryPressureSnapshot();
            var journalDisk = BuildJournalDiskSnapshot();
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
                JournalDisk = journalDisk,
                RetentionCleanup = retentionCleanup,
            };
        }

        private HealthJournalDiskSnapshot BuildJournalDiskSnapshot()
        {
            var usedBytes = _journal.UsedBytes;
            var maxBytes = _journal.MaxBytes;
            var highWaterBytes = _journal.HighWaterBytes;
            var state = JournalSegmentPolicy.EvaluatePressureState(usedBytes, highWaterBytes, maxBytes);
            return new HealthJournalDiskSnapshot(state, maxBytes, usedBytes, highWaterBytes, usedBytes >= maxBytes);
        }

        private HealthMemoryPressureSnapshot BuildMemoryPressureSnapshot()
        {
            var estimatedBytes = _memoryAccounting.ReadEstimatedBytes();
            var state = _memoryEvaluator.Evaluate(estimatedBytes);
            var pressureStateName = state switch
            {
                PressureLevel.Normal => "normal",
                PressureLevel.High => "high",
                PressureLevel.Critical => "critical",
                _ => throw new InvalidOperationException("Unsupported memory pressure state."),
            };

            return new HealthMemoryPressureSnapshot(
                pressureStateName,
                _memoryPressureOptions.MaxEstimatedCacheBytes,
                estimatedBytes,
                _memoryAccounting.ReadEntryCount(),
                _memoryAccounting.ReadRejectedWriteCount(),
                true);
        }

        private HealthRetentionCleanupSnapshot BuildRetentionCleanupSnapshot() => new(
            _retentionCleanup.IsDegraded,
            _retentionCleanup.ConsecutiveWriteFailures,
            _retentionCleanup.RecentFailureCount,
            _retentionCleanup.LastFailureUtc);
    }
}
