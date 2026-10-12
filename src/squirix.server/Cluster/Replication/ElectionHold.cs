using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Decides from the log status whether the election driver of a group must hold back at its start: the log is not ready, or its copy is not complete.</summary>
internal static class ElectionHold
{
    /// <summary>Decides whether the start of the own group must wait.</summary>
    /// <param name="status">The log status.</param>
    /// <returns>
    /// The outcome of the held start: silent for a log that is not ready, the <see cref="FailoverDenial.Incomplete" /> denial for a copy that
    /// awaits a state transfer, the <see cref="FailoverDenial.Unseeded" /> denial for any other copy that is not complete; <see langword="null" /> when the start may go on.
    /// </returns>
    internal static ElectionOutcome? OnStart(in FollowerLogStatus status) => true switch
    {
        _ when status.Readiness != FollowerLogReadiness.Ready => new ElectionOutcome(ElectionEvent.None, status.CurrentTerm),
        _ when status.MemberState == GroupMemberState.Incomplete => new ElectionOutcome(ElectionEvent.Denied, status.CurrentTerm, FailoverDenial.Incomplete),
        _ when status.MemberState != GroupMemberState.Complete => new ElectionOutcome(ElectionEvent.Denied, status.CurrentTerm, FailoverDenial.Unseeded),
        _ => null,
    };
}
