namespace Squirix.Server.Cluster.Replication;

/// <summary>The kind of a message that follows the begin of a state transfer.</summary>
internal enum ReplicaTransferStep
{
    /// <summary>A chunk of the snapshot image.</summary>
    Snapshot = 0,

    /// <summary>A step that removes keys of the group.</summary>
    Wipe = 1,

    /// <summary>A batch of entries.</summary>
    Entries = 2,

    /// <summary>The end of the transfer with the count and the checksum of the entries.</summary>
    End = 3,
}
