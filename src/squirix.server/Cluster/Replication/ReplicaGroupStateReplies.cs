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
        /// Only an answer from the follower's log is a contact: accepted, a log mismatch the leader repairs, a log not ready yet, or a copy that
        /// is incomplete and awaits a state transfer, which is alive; its reply carries the durable term of the copy, and a higher term is
        /// observed like in any other reply. A refusal before the log (another topology, another membership) proves nothing, and its term is
        /// still observed. A contact never counts toward a commit or a read index confirmation.
        /// </remarks>
        internal void RecordFollowerReply(int replicaIndex, in FollowerLogAppendResult reply)
        {
            var code = reply.RefusalCode;
            var fromLog = reply.Success ||
                          string.Equals(code, FollowerLogRefusal.LogMismatch, StringComparison.Ordinal) ||
                          string.Equals(code, FollowerLogRefusal.NotReady, StringComparison.Ordinal) ||
                          string.Equals(code, FollowerLogRefusal.Incomplete, StringComparison.Ordinal);
            if (fromLog)
                state.RecordFollowerContact(replicaIndex, reply.CurrentTerm);
            else
                state.ObserveHigherTerm(reply.CurrentTerm);
        }
    }
}
