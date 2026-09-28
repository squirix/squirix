namespace Squirix.Server.Cluster.Replication;

/// <summary>Schema version of the replication envelope header carried by every replication RPC.</summary>
internal static class EnvelopeCodec
{
    /// <summary>Gets the network schema version that replication RPC envelope headers must carry.</summary>
    internal const uint SchemaVersion = 1;
}
