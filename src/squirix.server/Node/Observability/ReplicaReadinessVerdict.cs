namespace Squirix.Server.Node.Observability;

/// <summary>Verdict for read-only replica-group readiness diagnostics.</summary>
internal enum ReplicaReadinessVerdict
{
    /// <summary>The group is ready to serve.</summary>
    Ready = 0,

    /// <summary>A higher term was observed; the old leader is fenced.</summary>
    StaleTerm = 1,

    /// <summary>The leader lost majority contact and fails closed.</summary>
    MinorityFenced = 2,

    /// <summary>The durable identity disagrees with the configured topology.</summary>
    TopologyMismatch = 3,

    /// <summary>The group log is not in durability readiness.</summary>
    LogNotReady = 4,

    /// <summary>The copy of the group on this node is not complete: it votes for nobody and takes no entries.</summary>
    Unseeded = 5,
}
