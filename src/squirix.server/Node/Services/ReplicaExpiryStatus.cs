namespace Squirix.Server.Node.Services;

/// <summary>What one batch expiry decided for one key.</summary>
internal enum ReplicaExpiryStatus
{
    /// <summary>The key holds no entry.</summary>
    Absent = 0,

    /// <summary>The key holds a live entry, which gets no tombstone.</summary>
    Live = 1,

    /// <summary>The entry is expired and its tombstone is committed.</summary>
    Tombstoned = 2,

    /// <summary>The entry is expired, but its tombstone is not committed: the outcome is unknown or the run was refused.</summary>
    Failed = 3,
}
