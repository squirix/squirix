using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Creates the follower senders of a replica group this node leads.</summary>
internal static class ReplicaFollowerSenders
{
    /// <summary>Creates the sender of every follower slot, seeded with the last entry of the leader log.</summary>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="members">Group members in slot order.</param>
    /// <param name="leaderReplicaIndex">Zero-based slot of this node, the leader, which gets no sender.</param>
    /// <param name="status">Durable log status of the leader.</param>
    /// <param name="header">Replication envelope identity for follower calls.</param>
    /// <param name="timing">The commit budget, the shutdown budget, and the time source of the follower request timeouts.</param>
    /// <param name="leakReporter">Reports, with the sender's shutdown budget, a sender that leaked a request it could not stop.</param>
    /// <returns>The senders of the follower slots, in slot order.</returns>
    internal static ReplicaFollowerSender[] Create(
        IReplicaRpcGateway gateway,
        string[] members,
        int leaderReplicaIndex,
        in FollowerLogStatus status,
        in ReplicaRpcHeader header,
        in SenderTiming timing,
        Action<TimeSpan> leakReporter)
    {
        var senders = new ReplicaFollowerSender[members.Length - 1];
        var senderShutdownBudget = timing.ShutdownBudget < ReplicaCommitCoordinator.DefaultShutdownBudget ? timing.ShutdownBudget : ReplicaCommitCoordinator.DefaultShutdownBudget;
        var slots = new ReplicaSlots(leaderReplicaIndex);
        for (var i = 0; i < senders.Length; i++)
        {
            senders[i] = new ReplicaFollowerSender(gateway, members[slots.SlotOf(i)], in header, status.LastLogIndex, status.LastLogTerm, timing.CommitBudget)
            {
                // The senders' teardown is part of the committer's dispose, so it never waits longer than the committer's budget.
                ShutdownBudget = senderShutdownBudget,
                ShutdownLeakReporter = leakReporter,
                TimeProvider = timing.TimeProvider,
            };
        }

        return senders;
    }

    /// <summary>Timing of the follower senders of one committer.</summary>
    /// <param name="CommitBudget">The longest wait for one follower request.</param>
    /// <param name="ShutdownBudget">The committer's shutdown budget; each sender's teardown never waits longer than the coordinator default.</param>
    /// <param name="TimeProvider">The time source of each follower request timeout.</param>
    [Immutable]
    internal readonly record struct SenderTiming(TimeSpan CommitBudget, TimeSpan ShutdownBudget, TimeProvider TimeProvider);
}
