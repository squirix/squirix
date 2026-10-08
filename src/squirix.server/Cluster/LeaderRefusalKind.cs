namespace Squirix.Server.Cluster;

/// <summary>Why a node refuses an operation on a replica group before anything is appended.</summary>
internal enum LeaderRefusalKind
{
    /// <summary>This node leads the group with authority: nothing is refused.</summary>
    None = 0,

    /// <summary>This node leads without authority because it saw a higher term: its term is stale.</summary>
    StaleTerm = 1,

    /// <summary>Another node is known to lead the group.</summary>
    StaleOwner = 2,

    /// <summary>No node is known to lead the group with authority; a retry may succeed later.</summary>
    NoLeader = 3,
}
