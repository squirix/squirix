using System;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Creates the follower senders of an owned replica group.</summary>
internal static class ReplicaFollowerSenders
{
    /// <summary>Creates the sender of every follower slot, seeded with the last entry of the leader log.</summary>
    /// <param name="gateway">Follower replication RPCs.</param>
    /// <param name="members">Ordered group members; index zero is this node.</param>
    /// <param name="status">Durable log status of the leader.</param>
    /// <param name="header">Replication envelope identity for follower calls.</param>
    /// <param name="commitBudget">The longest wait for one follower request.</param>
    /// <param name="shutdownBudget">The committer's shutdown budget; each sender's teardown never waits longer than the coordinator default.</param>
    /// <param name="leakReporter">Reports, with the sender's shutdown budget, a sender that leaked a request it could not stop.</param>
    /// <returns>The senders of slots one and up, in slot order.</returns>
    internal static ReplicaFollowerSender[] Create(
        IReplicaRpcGateway gateway,
        string[] members,
        in FollowerLogStatus status,
        in ReplicaRpcHeader header,
        TimeSpan commitBudget,
        TimeSpan shutdownBudget,
        Action<TimeSpan> leakReporter)
    {
        var senders = new ReplicaFollowerSender[members.Length - 1];
        var senderShutdownBudget = shutdownBudget < ReplicaCommitCoordinator.DefaultShutdownBudget ? shutdownBudget : ReplicaCommitCoordinator.DefaultShutdownBudget;
        for (var i = 0; i < senders.Length; i++)
        {
            senders[i] = new ReplicaFollowerSender(gateway, members[i + 1], in header, status.LastLogIndex, status.LastLogTerm, commitBudget)
            {
                // The senders' teardown is part of the committer's dispose, so it never waits longer than the committer's budget.
                ShutdownBudget = senderShutdownBudget,
                ShutdownLeakReporter = leakReporter,
            };
        }

        return senders;
    }
}
