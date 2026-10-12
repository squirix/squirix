using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Names of the member states the replica status reports.</summary>
[Immutable]
internal static class GroupMemberStateNames
{
    /// <summary>Maps a member state to the name the replica status reports.</summary>
    /// <param name="state">The member state.</param>
    /// <returns>The state name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The state is not a named value.</exception>
    internal static string Of(GroupMemberState state) => state switch
    {
        GroupMemberState.Unseeded => "unseeded",
        GroupMemberState.Complete => "complete",
        GroupMemberState.Incomplete => "incomplete",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unsupported group member state."),
    };
}
