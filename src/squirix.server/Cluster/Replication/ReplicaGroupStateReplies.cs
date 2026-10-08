using System;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The classification of follower replies posted to a <see cref="ReplicaGroupState" />.</summary>
internal static class ReplicaGroupStateReplies
{
    extension(ReplicaGroupState state)
    {
        /// <summary>Records a follower reply to an append or heartbeat of this leader, as a contact or as an observed term.</summary>
        /// <param name="replicaIndex">The slot of the follower.</param>
        /// <param name="reply">The reply.</param>
        /// <remarks>
        /// Only an answer from the follower's log is a contact: accepted, a log mismatch the leader repairs, or a log not ready yet. A refusal
        /// before the log (another topology, another membership) proves nothing, and its term is still observed.
        /// </remarks>
        internal void RecordFollowerReply(int replicaIndex, in FollowerLogAppendResult reply)
        {
            if (reply.Success || string.Equals(reply.RefusalCode, FollowerLogRefusal.LogMismatch, StringComparison.Ordinal) ||
                string.Equals(reply.RefusalCode, FollowerLogRefusal.NotReady, StringComparison.Ordinal))
                state.RecordFollowerContact(replicaIndex, reply.CurrentTerm);
            else
                state.ObserveHigherTerm(reply.CurrentTerm);
        }
    }
}
