namespace Squirix.Server.Cluster.Replication;

/// <summary>How one wipe step of a state transfer ended.</summary>
internal enum ReplicaWipeStep
{
    /// <summary>The step removed the keys it examined and the group may have more.</summary>
    More = 0,

    /// <summary>The group has no key left.</summary>
    Done = 1,

    /// <summary>The node journal refused a removal for now; no examined key is skipped and the same step may be sent again.</summary>
    JournalBusy = 2,
}
