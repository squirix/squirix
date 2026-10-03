namespace Squirix.Server.Cluster;

/// <summary>Describes the first ring mismatch a node detected.</summary>
/// <param name="PeerNodeId">The peer whose ring differs.</param>
/// <param name="Direction">Which side detected the mismatch.</param>
/// <param name="PeerFingerprint">The peer ring fingerprint, "missing" when the peer sent none, or "unknown" when it is not available.</param>
internal readonly record struct RingMismatchReport(string PeerNodeId, RingMismatchDirection Direction, string PeerFingerprint);
