namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of compacting a replica group log through an index.</summary>
internal enum GroupCompactionOutcome
{
    /// <summary>The snapshot covering the index is published and the log holds only the entries above it.</summary>
    Compacted = 0,

    /// <summary>
    /// The log is not ready, or the index is not both its commit and its applied index. A refusal after the snapshot was published
    /// leaves the log itself intact.
    /// </summary>
    NotReady = 1,

    /// <summary>Nothing changed: an entry at or below the index still has an unresolved idempotency outcome the snapshot could not carry.</summary>
    UnresolvedOutcome = 2,

    /// <summary>Nothing changed: the snapshot covering the index would exceed the configured maximum snapshot size.</summary>
    SnapshotTooLarge = 3,
}
