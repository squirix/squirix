namespace Squirix.Server.Node.Services;

/// <summary>Wire-stable mutation and record kinds for replicated cache operations.</summary>
/// <remarks>
/// The strings travel on the replication wire and in canonical log payloads; renaming one breaks
/// rolling compatibility between activated releases.
/// </remarks>
internal static class ReplicaMutationKinds
{
    /// <summary>Reads an adapter value and records any renewal and its canonical response.</summary>
    internal const string DistributedGet = "distributed-get";

    /// <summary>Renews an adapter value without retaining the value in its outcome.</summary>
    internal const string DistributedRefresh = "distributed-refresh";

    /// <summary>Removes a key whose deadline the leader found passed: the tombstone every replica applies before a read may miss.</summary>
    internal const string Expire = "expire";

    /// <summary>Changes no cache state: the entry a leader commits at the start of its term.</summary>
    internal const string LeaderNoop = "leader-noop";

    /// <summary>Removes a key.</summary>
    internal const string Remove = "remove";

    /// <summary>Clears the expiration of a key.</summary>
    internal const string RemoveExpiration = "removeexpiration";

    /// <summary>Writes an entry unconditionally.</summary>
    internal const string Set = "set";

    /// <summary>Refreshes the expiration of a key.</summary>
    internal const string Touch = "touch";

    /// <summary>Adds an entry only when the key is absent.</summary>
    internal const string TryAdd = "tryadd";

    /// <summary>Replaces the value of an existing key.</summary>
    internal const string Update = "update";
}
