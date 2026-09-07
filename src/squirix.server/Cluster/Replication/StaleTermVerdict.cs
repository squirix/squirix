namespace Squirix.Server.Cluster.Replication;

/// <summary>Verdict for a server response inspected for a stale term.</summary>
internal enum StaleTermVerdict
{
    /// <summary>The response carries no stale term.</summary>
    Current = 0,

    /// <summary>The response reports a stale term and authorizes at most one reroute.</summary>
    Stale = 1,
}
