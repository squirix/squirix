namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of capturing the group snapshot at the durable applied index.</summary>
internal enum GroupSnapshotCaptureOutcome
{
    /// <summary>The snapshot at the durable applied index is captured; nothing was written.</summary>
    Captured = 0,

    /// <summary>The log is disposed or not ready.</summary>
    NotReady = 1,

    /// <summary>The copy is not complete, so its applied index says nothing about the state of the group.</summary>
    NotComplete = 2,

    /// <summary>Nothing is applied yet, so there is no state to transfer.</summary>
    NothingApplied = 3,

    /// <summary>An entry at or below the applied index still has an unresolved idempotency outcome the snapshot could not carry.</summary>
    UnresolvedOutcome = 4,
}
