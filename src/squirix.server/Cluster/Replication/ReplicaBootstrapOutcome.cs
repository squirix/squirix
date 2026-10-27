using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The outcome of one iteration of the first start of a replica group.</summary>
/// <param name="Result">What the iteration did.</param>
/// <param name="NodeId">The member the result concerns; empty when it concerns no member.</param>
/// <param name="MemberState">The state of the copy the member reported when it refused the offer; <see langword="null" /> when it did not report one.</param>
/// <param name="RefusalCode">The refusal code of the member that refused the offer; empty for any other result.</param>
[Immutable]
internal readonly record struct ReplicaBootstrapOutcome(ReplicaBootstrapResult Result, string NodeId, GroupMemberState? MemberState = null, string RefusalCode = "")
{
    /// <summary>Gets a value indicating whether the first start has nothing more to do for the group: its copy is complete or incomplete.</summary>
    internal bool IsFinal => Result is ReplicaBootstrapResult.Completed or ReplicaBootstrapResult.AlreadyComplete or ReplicaBootstrapResult.Incomplete;

    /// <summary>Gets the name of the state the member reported, as the log prints it.</summary>
    /// <exception cref="InvalidOperationException">The state is not a named value.</exception>
    internal string MemberStateName => MemberState switch
    {
        GroupMemberState.Unseeded => "unseeded",
        GroupMemberState.Complete => "complete",
        GroupMemberState.Incomplete => "incomplete",
        null => "not reported",
        _ => throw new InvalidOperationException("Unsupported group member state."),
    };
}
