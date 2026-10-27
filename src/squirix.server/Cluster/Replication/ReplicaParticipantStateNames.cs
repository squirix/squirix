using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Names of the participation states the readiness details report.</summary>
[Immutable]
internal static class ReplicaParticipantStateNames
{
    /// <summary>Maps a participation state to the name the readiness details report.</summary>
    /// <param name="state">The participation state.</param>
    /// <returns>The state name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The state is not a named value.</exception>
    internal static string Of(ReplicaParticipantState state) => state switch
    {
        ReplicaParticipantState.Recovering => "recovering",
        ReplicaParticipantState.CatchingUp => "catching-up",
        ReplicaParticipantState.Ready => "ready",
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unsupported participant state."),
    };
}
