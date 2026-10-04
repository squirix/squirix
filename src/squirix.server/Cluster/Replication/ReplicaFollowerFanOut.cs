using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Starts the follower appends of one entry and records their acknowledgements until a durable majority backs it.</summary>
internal static class ReplicaFollowerFanOut
{
    /// <summary>Starts the append of the entry on every follower and observes each started append.</summary>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="pipeline">Pipeline that appends to followers and records lagging replicas.</param>
    /// <param name="mutation">The entry being committed.</param>
    /// <param name="pending">Receives the follower observations; filled in place so the caller can observe the ones started before a failure.</param>
    /// <param name="pendingReplicaIndexes">Receives the slots of <paramref name="pending" />.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    internal static void StartFollowers(
        ReplicaCommitQuorum quorum,
        IReplicaCommitPipeline pipeline,
        PreparedReplicaMutation mutation,
        List<Task<FollowerCompletion>> pending,
        HashSet<int> pendingReplicaIndexes,
        CancellationToken cancellationToken)
    {
        var followerTasks = new HashSet<Task<ReplicaDurableAcknowledgement>>(ReferenceEqualityComparer.Instance);
        for (var replicaIndex = 1; replicaIndex < quorum.ReplicaCount; replicaIndex++)
        {
            var followerTask = pipeline.AppendFollowerAsync(replicaIndex, mutation, cancellationToken).AsTask();
            if (!followerTasks.Add(followerTask))
            {
                pipeline.RecordLaggingReplica(replicaIndex, mutation.LogIndex);
                continue;
            }

            pending.Add(ReplicaFollowerObservation.AwaitFollowerAsync(replicaIndex, followerTask));
            _ = pendingReplicaIndexes.Add(replicaIndex);
        }
    }

    /// <summary>Records a follower completion, or marks the replica lagging when it did not acknowledge.</summary>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="pipeline">Pipeline that appends to followers and records lagging replicas.</param>
    /// <param name="follower">The follower completion.</param>
    /// <param name="mutation">The entry being committed.</param>
    internal static void RecordAcknowledgement(ReplicaCommitQuorum quorum, IReplicaCommitPipeline pipeline, in FollowerCompletion follower, PreparedReplicaMutation mutation)
    {
        if (follower.Acknowledgement is { } acknowledgement && quorum.TryRecord(follower.ReplicaIndex, in acknowledgement, mutation))
            return;

        pipeline.RecordLaggingReplica(follower.ReplicaIndex, mutation.LogIndex);
    }

    /// <summary>Records follower completions of the entry until a majority backs it, no follower or progress that could complete it is left, or the token ends.</summary>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="pipeline">Pipeline that records lagging replicas.</param>
    /// <param name="pending">The follower observations of the entry that did not complete yet.</param>
    /// <param name="pendingReplicaIndexes">The slots of <paramref name="pending" />.</param>
    /// <param name="mutation">The entry being committed.</param>
    /// <param name="commitIndex">The group commit index, which does not change while the caller holds the commit gate.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    /// <returns>An asynchronous operation; the caller checks whether the majority was reached.</returns>
    /// <remarks>Call only under the coordinator's commit gate, so <paramref name="commitIndex" /> cannot go stale.</remarks>
    internal static async Task AwaitMajorityAsync(
        ReplicaCommitQuorum quorum,
        IReplicaCommitPipeline pipeline,
        List<Task<FollowerCompletion>> pending,
        HashSet<int> pendingReplicaIndexes,
        PreparedReplicaMutation mutation,
        ulong commitIndex,
        CancellationToken cancellationToken)
    {
        // Rebuilt only once a follower completion is consumed, so wakes from other paths do not stack continuations on every follower task.
        var next = pending.Count > 0 ? Task.WhenAny(pending) : null;
        while (true)
        {
            // Read before the majority check, so an acknowledgement recorded after the check wakes the wait below.
            var progressVersion = quorum.ProgressVersion;
            if (quorum.FindCommitIndex(commitIndex, mutation.LogIndex) >= mutation.LogIndex)
                return;

            if (next == null)
            {
                // No follower of this entry is left to answer, but an acknowledgement buffered behind a prefix that an earlier entry's
                // observation still records can complete the majority: wait for that progress, within the budget, before giving up.
                if (!quorum.HasBufferedThrough(mutation.LogIndex))
                    return;

                await quorum.WaitForProgressAsync(progressVersion, cancellationToken).ConfigureAwait(false);
                continue;
            }

            var progress = quorum.WaitForProgressAsync(progressVersion, cancellationToken);
            if (await Task.WhenAny(next, progress).ConfigureAwait(false) == progress)
            {
                await progress.ConfigureAwait(false);
                continue;
            }

            var completed = await next.ConfigureAwait(false);
            _ = pending.Remove(completed);
            var follower = await completed.ConfigureAwait(false);
            _ = pendingReplicaIndexes.Remove(follower.ReplicaIndex);
            RecordAcknowledgement(quorum, pipeline, in follower, mutation);
            next = pending.Count > 0 ? Task.WhenAny(pending) : null;
        }
    }
}
