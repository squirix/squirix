namespace Squirix.Server.Core;

/// <summary>Who decides that a cache entry has expired and may be dropped from the node.</summary>
internal enum CacheExpiryAuthority
{
    /// <summary>The node clock: an entry past its deadline reads as absent and is dropped by reads, snapshots, recovery and compaction.</summary>
    LocalClock = 1,

    /// <summary>
    /// Committed records only: storage keeps an entry past its deadline until a committed record removes it, so only the replica group
    /// leader decides expiry and every replica drops the entry at the same log position.
    /// </summary>
    CommittedRecords = 2,
}
