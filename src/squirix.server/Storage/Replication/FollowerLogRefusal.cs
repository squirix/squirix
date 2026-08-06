namespace Squirix.Server.Storage.Replication;

/// <summary>Stable refusal markers returned by the follower log before any journal mutation.</summary>
/// <remarks>
/// These strings are the storage-side twin of the closed wire refusal codes. The values must stay identical
/// to <c>Squirix.Server.Cluster.Replication.RefusalCodes</c>; the storage layer may not reference the cluster
/// namespace, so the constants are mirrored here and the transport adapter maps them through.
/// </remarks>
internal static class FollowerLogRefusal
{
    /// <summary>The caller advertised a lower term than the durably persisted current term.</summary>
    internal const string StaleTerm = "stale-term";

    /// <summary>The append would skip an index, or an existing entry holds different canonical bytes.</summary>
    internal const string LogMismatch = "log-mismatch";

    /// <summary>The group is not in the local static composition, or the log is not yet ready.</summary>
    internal const string NotReady = "not-ready";

    /// <summary>A frame checksum failed during startup validation.</summary>
    internal const string ChecksumMismatch = "checksum-mismatch";

    /// <summary>The request targets a different topology fingerprint or configuration generation.</summary>
    internal const string TopologyMismatch = "topology-mismatch";

    /// <summary>The request targets a group this node does not participate in.</summary>
    internal const string NotMember = "not-member";
}
