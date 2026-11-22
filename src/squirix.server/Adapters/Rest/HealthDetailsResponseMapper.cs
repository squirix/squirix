using System.Collections.Generic;
using Squirix.Server.Runtime.Diagnostics;

namespace Squirix.Server.Adapters.Rest;

internal static class HealthDetailsResponseMapper
{
    internal static HealthReadyDetailsResponse Build(HealthReadyDetailsSnapshot snapshot)
    {
        var clientPool = new HealthClientPoolDetails(snapshot.ClientPool.Enabled, snapshot.ClientPool.PeerCount);
        var coordination = new HealthCoordinationDetails(
            new HealthLeaseDetails(
                snapshot.Coordination.Lease.Enabled,
                snapshot.Coordination.Lease.ActiveLeases,
                snapshot.Coordination.Lease.PendingGrants,
                snapshot.Coordination.Lease.PendingReleases),
            new HealthWatchDetails(
                snapshot.Coordination.Watch.Enabled,
                snapshot.Coordination.Watch.ActiveWatches,
                snapshot.Coordination.Watch.DroppedEvents,
                snapshot.Coordination.Watch.BufferedEvents));
        var memoryPressure = new HealthMemoryPressureDetails(
            snapshot.MemoryPressure.State,
            snapshot.MemoryPressure.MaxEstimatedCacheBytes,
            snapshot.MemoryPressure.EstimatedBytes,
            snapshot.MemoryPressure.EntryCount,
            snapshot.MemoryPressure.RejectedWriteCount,
            snapshot.MemoryPressure.WriteRejectionActive);
        var journalDisk = new HealthJournalDiskDetails(
            snapshot.JournalDisk.State,
            snapshot.JournalDisk.MaxBytes,
            snapshot.JournalDisk.UsedBytes,
            snapshot.JournalDisk.HighWaterBytes,
            snapshot.JournalDisk.WriteRejectionActive);
        var retentionCleanup = new HealthRetentionCleanupDetails(
            snapshot.RetentionCleanup.Degraded,
            snapshot.RetentionCleanup.ConsecutiveWriteFailures,
            snapshot.RetentionCleanup.RecentFailureCount,
            snapshot.RetentionCleanup.LastFailureUtc);
        return new HealthReadyDetailsResponse(
            snapshot.JournalBacklogOps,
            snapshot.SnapshotAgeSeconds,
            snapshot.SnapshotInFlight,
            new HealthReadyDetailSections(clientPool, coordination, memoryPressure, retentionCleanup, journalDisk, BuildReplicaGroupDetails(snapshot.ReplicaGroups)));
    }

    private static HealthReplicaGroupDetails[] BuildReplicaGroupDetails(IReadOnlyList<HealthReplicaGroupSnapshot> groups)
    {
        var details = new HealthReplicaGroupDetails[groups.Count];
        for (var i = 0; i < details.Length; i++)
        {
            var group = groups[i];
            var copies = new HealthReplicaCopyDetails[group.Copies.Count];
            for (var c = 0; c < copies.Length; c++)
                copies[c] = new HealthReplicaCopyDetails(group.Copies[c].PeerId, group.Copies[c].State, group.Copies[c].LagEntries);

            details[i] = new HealthReplicaGroupDetails(group.GroupId, group.LogBytes, group.RetainedEntries, group.SnapshotIndex, copies);
        }

        return details;
    }
}
