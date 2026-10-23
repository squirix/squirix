using Squirix.Server.Cluster;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>The term a node leads its own replica group in when no election made it the leader.</summary>
/// <remarks>
/// Static leadership is term one and nothing else: a group log whose term moved past it was raised by an election, and leading that term
/// without votes would let two leaders append in one term.
/// </remarks>
internal static class StaticLeaderTerm
{
    /// <summary>Resolves the term a static leader leads the group in.</summary>
    /// <param name="status">Durable log status of the group.</param>
    /// <param name="term">The static term when the log never moved past it; otherwise zero.</param>
    /// <returns><see langword="true" /> when the log term is at most the static term.</returns>
    internal static bool TryResolve(in FollowerLogStatus status, out ulong term)
    {
        var unraised = status.CurrentTerm <= StaticLeaderTable.StaticTerm;
        term = unraised ? StaticLeaderTable.StaticTerm : 0;
        return unraised;
    }
}
