namespace Squirix.Server.Cluster.Replication;

/// <summary>Stable closed refusal markers for the internal replication wire.</summary>
/// <remarks>
/// These are the wire-side twin of the storage refusal constants mirrored in
/// <c language="csharp">Squirix.Server.Storage.Replication.FollowerLogRefusal</c>. The values must stay identical; a guard
/// test asserts the mirror in both directions.
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

    /// <summary>A frame checksum failed during startup validation.</summary>
    internal const string ChecksumMismatch = "checksum-mismatch";

    /// <summary>The request targets a different topology fingerprint or configuration generation.</summary>
    internal const string TopologyMismatch = "topology-mismatch";

    /// <summary>The request targets a group this node does not participate in.</summary>
    internal const string NotMember = "not-member";
}
