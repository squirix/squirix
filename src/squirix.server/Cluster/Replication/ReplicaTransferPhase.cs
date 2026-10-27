namespace Squirix.Server.Cluster.Replication;

/// <summary>The step a state transfer expects next.</summary>
internal enum ReplicaTransferPhase
{
    /// <summary>The snapshot image is still being received.</summary>
    Snapshot = 0,

    /// <summary>The snapshot is decoded; the keys of the group are being wiped.</summary>
    Wipe = 1,

    /// <summary>The group holds no key; entries are being written.</summary>
    Entries = 2,
}
