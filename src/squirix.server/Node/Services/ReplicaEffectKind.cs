namespace Squirix.Server.Node.Services;

/// <summary>What applying a replicated record does to local memory, as decided by the leader at prepare time.</summary>
internal enum ReplicaEffectKind
{
    /// <summary>Writes the exact entry the record carries, deadline, version and tags included.</summary>
    Upsert = 1,

    /// <summary>Removes the key.</summary>
    Delete = 2,

    /// <summary>Leaves memory as it is: the leader found nothing to change.</summary>
    Unchanged = 3,

    /// <summary>Touches no cache at all: a leader-term no-op, which names no cache and no key.</summary>
    None = 4,
}
