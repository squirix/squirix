namespace Squirix.Server.Storage.Replication;

/// <summary>State surface the appending, recovery, and durable-write coordinators require from the owning follower log.</summary>
/// <remarks>
/// Declaring this interface lets the static coordinator classes (FollowerLogAppend, FollowerLogRecovery,
/// FollowerLogDurable) depend on an abstraction; the storage surface is passed separately as <see cref="FollowerLogJournal" /> instead of the concrete <see cref="FollowerLog" />, breaking the
/// mutual type dependency flagged by ND1409 through the dependency inversion principle.
/// </remarks>
internal interface IFollowerLogContext : IFollowerLogState, IFollowerLogDurability
{
    /// <summary>
    /// Installs the snapshot baseline without pruning the retained indexes; reserved for paths whose index
    /// lifecycle is owned separately (snapshot publication retains the covered prefix until the applied
    /// watermark releases it, while recovery and installation rebuild both indexes afterward).
    /// </summary>
    /// <param name="baseline">The restored snapshot baseline.</param>
    void RestoreBaseline(SnapshotBaseline baseline);
}
