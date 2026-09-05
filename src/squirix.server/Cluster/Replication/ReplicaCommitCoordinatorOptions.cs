using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Fixed group configuration for <see cref="ReplicaCommitCoordinator" />.</summary>
/// <param name="ReplicaCount">Fixed replica count, including the leader.</param>
/// <param name="InitialLogIndex">Last durable log index at coordinator start.</param>
/// <param name="InitialCommitIndex">Durable group commit index at coordinator start.</param>
/// <param name="MaxInFlight">Maximum concurrently prepared mutations.</param>
internal sealed record ReplicaCommitCoordinatorOptions(int ReplicaCount, ulong InitialLogIndex, ulong InitialCommitIndex, int MaxInFlight)
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
    internal ulong InitialCommitIndex { get; } = InitialCommitIndex == InitialLogIndex
        ? InitialCommitIndex
        : throw new ArgumentException("The durable log tail must be reconciled to the commit index before the coordinator starts.", nameof(InitialLogIndex));
}
