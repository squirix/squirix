namespace Squirix.Server.Cluster.Replication;

/// <summary>Schema of the replication envelope header carried by every replication RPC.</summary>
internal static class EnvelopeSchema
{
    /// <summary>Gets the network schema version that replication RPC envelope headers must carry.</summary>
    internal const uint Version = 1;
}
