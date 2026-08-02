namespace Squirix.Server.Cluster.Replication;

/// <summary>Closed replication envelope fields shared by wire and durable group storage.</summary>
/// <param name="SchemaVersion">Durable/network schema version.</param>
/// <param name="GroupId">Logical original-owner group identity.</param>
/// <param name="TopologyFingerprint">Static topology fingerprint bytes.</param>
/// <param name="ConfigurationGeneration">Stopped-topology configuration generation.</param>
/// <param name="Term">Group term.</param>
/// <param name="LeaderNodeId">Claimed leader node id (must match mTLS identity separately).</param>
/// <param name="SenderNodeId">Claimed sender node id (must match mTLS identity).</param>
/// <param name="LogIndex">Log index when applicable.</param>
/// <param name="CommitIndex">Commit index when applicable.</param>
/// <param name="PayloadChecksum">CRC32C over canonical payload bytes.</param>
internal sealed record Envelope(
    uint SchemaVersion,
    string GroupId,
    byte[] TopologyFingerprint,
    ulong ConfigurationGeneration,
    ulong Term,
    string LeaderNodeId,
    string SenderNodeId,
    ulong LogIndex,
    ulong CommitIndex,
    uint PayloadChecksum);
