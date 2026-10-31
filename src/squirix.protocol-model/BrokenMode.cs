namespace Squirix.ProtocolModel;

/// <summary>Broken-rule injection modes for negative explorer runs.</summary>
internal enum BrokenMode
{
    /// <summary>No broken rule; expect safety invariants to hold.</summary>
    None = 0,

    /// <summary>Grant a second vote in a term already voted in; the up-to-date log check still applies.</summary>
    Vote = 1,

    /// <summary>Commit an entry of an earlier term once a majority stores it, without an entry of the leader's term.</summary>
    CurrentTermCommit = 2,

    /// <summary>Mark read-index ready without majority confirm / apply wait.</summary>
    ReadIndex = 3,

    /// <summary>Repair a member that lost its disk without the term ceiling taken from a majority.</summary>
    RepairCeiling = 4,

    /// <summary>Grant a vote to a candidate whose log is longer, comparing the length of the logs before the term of their last entries.</summary>
    LogOrder = 5,

    /// <summary>Send the transfer hint from a leader that is not leaving, so a successor campaigns while the leader still serves.</summary>
    TransferHint = 6,

    /// <summary>
    /// Honour a transfer hint whose term the receiver has already moved past, so a replayed hint the successor normally
    /// ignores starts a second term. This is the stale-hint check removed and nothing else, and its purpose is to be
    /// caught: a replay test that passes under this mode is not testing replay protection.
    /// </summary>
    TransferHintStaleAccept = 7,
}
