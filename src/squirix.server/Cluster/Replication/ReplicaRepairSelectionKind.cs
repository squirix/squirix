namespace Squirix.Server.Cluster.Replication;

/// <summary>Kind of payload selected for the next follower repair step.</summary>
internal enum ReplicaRepairSelectionKind
{
    /// <summary>Replay retained entries.</summary>
    Entries = 0,

    /// <summary>Install a published snapshot before replay.</summary>
    Snapshot = 1,
}
