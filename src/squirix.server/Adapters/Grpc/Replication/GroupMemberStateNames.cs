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

    /// <summary>Maps the name a replica status reports to a member state.</summary>
    /// <param name="name">The state name.</param>
    /// <returns>The member state; <see langword="null" /> for an empty or unknown name.</returns>
    internal static GroupMemberState? Parse(string name) => name switch
    {
        "unseeded" => GroupMemberState.Unseeded,
        "complete" => GroupMemberState.Complete,
        "incomplete" => GroupMemberState.Incomplete,
        _ => null,
    };
}
