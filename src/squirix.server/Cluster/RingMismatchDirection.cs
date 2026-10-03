namespace Squirix.Server.Cluster;

/// <summary>Which side of a call detected a ring mismatch.</summary>
internal enum RingMismatchDirection
{
    /// <summary>A peer sent an internal owner call with a missing or different ring fingerprint.</summary>
    Inbound = 0,

    /// <summary>The key owner refused a forwarded call because the ring of this node differs.</summary>
    Outbound = 1,
}
