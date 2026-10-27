namespace Squirix.Server.Storage.Replication;

/// <summary>Stable refusal markers returned by the follower log before any journal mutation.</summary>
/// <remarks>
/// These strings are the storage-side twin of the closed wire refusal codes. The values must stay identical
/// to <c language="csharp">Squirix.Server.Cluster.Replication.RefusalCodes</c>; the storage layer may not reference the cluster
/// namespace, so the constants are mirrored here and the transport adapter maps them through.
/// </remarks>
internal static class FollowerLogRefusal
{
    /// <summary>The caller advertised a lower term than the durably persisted current term.</summary>
    internal const string StaleTerm = "stale-term";

    /// <summary>The append would skip an index, an entry carries a term above the request term, or an existing entry holds different canonical bytes.</summary>
    internal const string LogMismatch = "log-mismatch";

    /// <summary>
    /// The group is not in the local static composition, the log is not yet ready, or the requested
    /// commit or applied watermark exceeds the currently durable or committed state.
    /// </summary>
    internal const string NotReady = "not-ready";

    /// <summary>A frame checksum failed during startup validation.</summary>
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
}
