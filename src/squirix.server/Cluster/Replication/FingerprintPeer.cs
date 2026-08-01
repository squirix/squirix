namespace Squirix.Server.Cluster.Replication;

/// <summary>One peer contribution to the canonical topology fingerprint.</summary>
/// <param name="NodeId">Peer node identifier.</param>
/// <param name="ClientUri">Canonical client-facing origin URI.</param>
/// <param name="InterNodeUri">Effective inter-node origin URI (or internal-port derived URI).</param>
internal readonly record struct FingerprintPeer(string NodeId, string ClientUri, string InterNodeUri);
