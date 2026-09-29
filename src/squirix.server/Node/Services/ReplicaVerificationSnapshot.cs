using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The follower probing of one verification pass, taken without the commit gate.</summary>
/// <remarks>
/// It is either a final verdict, or the state the admission under the commit gate continues from. The admitting caller owns the arrays
/// and updates them in place; they are not copied.
/// </remarks>
[Mutable]
internal sealed class ReplicaVerificationSnapshot
{
    /// <summary>Initializes a new instance of the <see cref="ReplicaVerificationSnapshot" /> class that ends the pass.</summary>
    /// <param name="verdict">The verification state.</param>
    internal ReplicaVerificationSnapshot(ReplicaVerification verdict)
    {
        Verdict = verdict;
        Probed = [];
        Answered = [];
        Members = [];
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaVerificationSnapshot" /> class that continues into the admission.</summary>
    /// <param name="status">The log status the verdicts were built from.</param>
    /// <param name="probed">Per-slot verdicts.</param>
    /// <param name="answered">Slots whose follower answered.</param>
    /// <param name="members">Group members.</param>
    /// <param name="header">Replication envelope identity.</param>
    internal ReplicaVerificationSnapshot(FollowerLogStatus status, ReplicaProbeResult[] probed, bool[] answered, string[] members, ReplicaRpcHeader header)
    {
        Status = status;
        Probed = probed;
        Answered = answered;
        Members = members;
        Header = header;
    }

    /// <summary>Gets the slots whose follower answered, probed again when the leader tail moved before the admission.</summary>
    internal bool[] Answered { get; }

    /// <summary>Gets the replication envelope identity of the follower calls.</summary>
    internal ReplicaRpcHeader Header { get; }

    /// <summary>Gets the group members; index zero is this node.</summary>
    internal string[] Members { get; }

    /// <summary>Gets the per-slot verdicts.</summary>
    internal ReplicaProbeResult[] Probed { get; }

    /// <summary>Gets the log status the verdicts were built from.</summary>
    internal FollowerLogStatus Status { get; }

    /// <summary>Gets the final verification state, or <see langword="null" /> when the admission under the commit gate must continue.</summary>
    internal ReplicaVerification? Verdict { get; }
}
