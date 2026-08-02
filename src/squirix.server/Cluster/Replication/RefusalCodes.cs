namespace Squirix.Server.Cluster.Replication;

/// <summary>Stable closed refusal markers for the internal replication wire.</summary>
internal static class RefusalCodes
{
    internal const string ChecksumMismatch = "checksum-mismatch";

    internal const string LogMismatch = "log-mismatch";

    internal const string NotMember = "not-member";

    internal const string NotReady = "not-ready";

    internal const string StaleTerm = "stale-term";

    internal const string TopologyMismatch = "topology-mismatch";
}
