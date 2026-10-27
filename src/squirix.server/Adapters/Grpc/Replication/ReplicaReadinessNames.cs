using System;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Adapters.Grpc.Replication;

/// <summary>Names of the log readiness the replica status reports.</summary>
[Immutable]
internal static class ReplicaReadinessNames
{
    /// <summary>The name of a ready log.</summary>
    internal const string Ready = "ready";

    /// <summary>Maps a log readiness to the name the replica status reports.</summary>
    /// <param name="readiness">The log readiness.</param>
    /// <returns>The readiness name.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The readiness is not a named value.</exception>
    internal static string Of(FollowerLogReadiness readiness) => readiness switch
    {
        FollowerLogReadiness.Ready => Ready,
        FollowerLogReadiness.Failed => "failed",
        FollowerLogReadiness.Unknown => "unknown",
        _ => throw new ArgumentOutOfRangeException(nameof(readiness), readiness, "Unsupported follower readiness."),
    };
}
