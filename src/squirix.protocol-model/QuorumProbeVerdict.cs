namespace Squirix.ProtocolModel;

internal enum QuorumProbeVerdict
{
    /// <summary>The follower verified the probed tail.</summary>
    Accepted = 0,

    /// <summary>The follower answered in the term but could not verify the tail.</summary>
    LogMismatch = 1,

    /// <summary>The follower retained its term and vote but omitted log verification while repairing.</summary>
    Incomplete = 2,

    /// <summary>The answer proves no contact but may carry a newer term.</summary>
    Refused = 3,
}
