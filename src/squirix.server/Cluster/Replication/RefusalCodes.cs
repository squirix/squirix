namespace Squirix.Server.Cluster.Replication;

/// <summary>Stable closed refusal markers for the internal replication wire.</summary>
/// <remarks>
/// These are the wire-side twin of the storage refusal constants mirrored in
/// <c language="csharp">Squirix.Server.Storage.Replication.FollowerLogRefusal</c>. The values must stay identical; a guard
/// test asserts the mirror in both directions. <see cref="LeaderContact" />, <see cref="TransferBusy" />, <see cref="TransferStale" />, <see cref="JournalBusy" /> and <see cref="JournalFull" /> have no twin: the log never decides them.
/// </remarks>
internal static class RefusalCodes
{
    /// <summary>The caller advertised a lower term than the durably persisted current term.</summary>
    internal const string StaleTerm = "stale-term";

    /// <summary>The append would skip an index, or an existing entry holds different canonical bytes.</summary>
    internal const string LogMismatch = "log-mismatch";

    /// <summary>
    /// The group is not in the local static composition, the log is not yet ready, or the requested
    /// commit or applied watermark exceeds the currently durable or committed state.
    /// </summary>
    internal const string NotReady = "not-ready";

    /// <summary>A frame checksum failed during startup validation, or an appended entry is not a canonical replica record at its position.</summary>
    internal const string ChecksumMismatch = "checksum-mismatch";

    /// <summary>The request targets a different topology fingerprint or configuration generation.</summary>
    internal const string TopologyMismatch = "topology-mismatch";

    /// <summary>The request targets a group this node does not participate in.</summary>
    internal const string NotMember = "not-member";

    /// <summary>The current term vote was already granted to a different candidate.</summary>
    internal const string AlreadyVoted = "already-voted";

    /// <summary>The candidate log is behind the voter log and cannot win the election.</summary>
    internal const string StaleLog = "stale-log";

    /// <summary>The copy of the group on this node is not complete, so it takes no entries, grants no vote and adopts no term.</summary>
    internal const string Unseeded = "unseeded";

    /// <summary>The copy of the group on this node is marked incomplete and awaits a state transfer, so it takes no entries and adopts no term from a leader; it still answers ballots.</summary>
    internal const string Incomplete = "incomplete";

    /// <summary>The voter heard from a live leader of the group within the election timeout, so it refuses the pre-vote.</summary>
    /// <remarks>Decided by the election state before the log is reached, so it has no storage twin.</remarks>
    internal const string LeaderContact = "leader-contact";

    /// <summary>The group already holds a state transfer, so a bootstrap offer, a completion or another transfer is refused until it ends.</summary>
    /// <remarks>Decided by the transfer receiver before the log is reached, so it has no storage twin.</remarks>
    internal const string TransferBusy = "transfer-busy";

    /// <summary>The message names another transfer than the one the group holds, or arrives out of sequence; nothing changed.</summary>
    /// <remarks>Decided by the transfer receiver before the log is reached, so it has no storage twin.</remarks>
    internal const string TransferStale = "transfer-stale";

    /// <summary>The node journal of the follower is at its high-water mark, so the transfer step is refused for now and the same message may be sent again.</summary>
    /// <remarks>Decided by the transfer receiver before the log is reached, so it has no storage twin. The transfer is kept; nothing of the refused step is lost.</remarks>
    internal const string JournalBusy = "journal-busy";

    /// <summary>The node journal of the follower is full and cannot be freed, so waiting does not help and the transfer ends.</summary>
    /// <remarks>Decided by the transfer receiver (or by the leader once the follower stayed busy for the stall budget), so it has no storage twin.</remarks>
    internal const string JournalFull = "journal-full";
}
