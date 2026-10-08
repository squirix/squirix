using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Fixed group configuration for <see cref="ReplicaCommitCoordinator" />.</summary>
/// <param name="ReplicaCount">Fixed replica count, including the leader.</param>
/// <param name="InitialLogIndex">Last durable log index at coordinator start.</param>
/// <param name="InitialCommitIndex">Durable group commit index at coordinator start.</param>
/// <param name="MaxInFlight">Maximum concurrently prepared mutations.</param>
/// <param name="LeaderReplicaIndex">Zero-based replica slot of the leader; zero unless given.</param>
[Immutable]
internal sealed record ReplicaCommitCoordinatorOptions(int ReplicaCount, ulong InitialLogIndex, ulong InitialCommitIndex, int MaxInFlight, int LeaderReplicaIndex = 0)
{
    /// <summary>Gets the fixed replica count, including the leader.</summary>
    internal int ReplicaCount { get; } = ReplicaCount >= 2
        ? ReplicaCount
        : throw new ArgumentOutOfRangeException(nameof(ReplicaCount), "The majority coordinator is reserved for RF greater than one.");

    /// <summary>Gets the last durable log index at coordinator start.</summary>
    internal ulong InitialLogIndex { get; } = InitialCommitIndex <= InitialLogIndex
        ? InitialLogIndex
        : throw new ArgumentOutOfRangeException(nameof(InitialCommitIndex), "Commit index cannot exceed the last log index.");

    /// <summary>Gets the durable group commit index at coordinator start.</summary>
    /// <remarks>Entries above it form an uncommitted tail, which the coordinator must be given as a <see cref="ReplicaRecoveredTail" />.</remarks>
    internal ulong InitialCommitIndex { get; } = InitialCommitIndex;

    /// <summary>Gets the zero-based replica slot of the leader, whose own durable log counts toward the majority; zero unless given.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The slot is not a slot of the group.</exception>
    internal int LeaderReplicaIndex { get; } = LeaderReplicaIndex >= 0 && LeaderReplicaIndex < ReplicaCount
        ? LeaderReplicaIndex
        : throw new ArgumentOutOfRangeException(nameof(LeaderReplicaIndex), LeaderReplicaIndex, "The leader slot must be a slot of the group.");
}
