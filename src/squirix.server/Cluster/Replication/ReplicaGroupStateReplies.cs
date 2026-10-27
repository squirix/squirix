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
        /// still observed. A contact never counts toward a commit or a read index confirmation. An accepted append, a log mismatch, or an
        /// incomplete copy also counts as an electable contact, because such a log votes; only the first two can stand for election, and a log
        /// that is not ready refuses every ballot.
        /// </remarks>
        internal void RecordFollowerReply(int replicaIndex, in FollowerLogAppendResult reply)
        {
            var ready = IsReadyLog(in reply);
            var voter = ready || string.Equals(reply.RefusalCode, FollowerLogRefusal.Incomplete, StringComparison.Ordinal);
            if (!voter && !string.Equals(reply.RefusalCode, FollowerLogRefusal.NotReady, StringComparison.Ordinal))
            {
                state.ObserveHigherTerm(reply.CurrentTerm);
                return;
            }

            // A log that is not ready refuses every ballot; an incomplete copy votes but never stands for election.
            if (state.RecordFollowerContact(replicaIndex, reply.CurrentTerm) && voter)
                state.ElectableContacts.Record(replicaIndex, ready);

            // Only an accepted append or a log mismatch comes from a log that can stand for election.
            static bool IsReadyLog(in FollowerLogAppendResult reply)
            {
                return reply.Success || string.Equals(reply.RefusalCode, FollowerLogRefusal.LogMismatch, StringComparison.Ordinal);
            }
        }
    }
}
