using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Starts the follower appends of one entry and records their acknowledgements until a durable majority backs it.</summary>
internal static class ReplicaFollowerFanOut
{
    /// <summary>Starts the append of the entry on every follower and observes each started append.</summary>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="leaderReplicaIndex">Zero-based slot of the leader, which is never sent to.</param>
    /// <param name="pipeline">Pipeline that appends to followers and records lagging replicas.</param>
    /// <param name="run">The entries being committed, in log order.</param>
    /// <param name="pending">Receives the follower observations; filled in place so the caller can observe the ones started before a failure.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    internal static void StartFollowers(
        ReplicaCommitQuorum quorum,
        int leaderReplicaIndex,
        IReplicaCommitPipeline pipeline,
        PreparedReplicaMutation[] run,
        List<Task<FollowerCompletion>> pending,
        CancellationToken cancellationToken)
    {
        var last = run[^1];
        var startedTasks = new Task<ReplicaDurableAcknowledgement>[quorum.ReplicaCount];
        var startedCount = 0;
        var slots = new ReplicaSlots(leaderReplicaIndex);
        for (var replicaIndex = 0; replicaIndex < quorum.ReplicaCount; replicaIndex++)
        {
            if (!slots.IsFollower(replicaIndex))
                continue;

            var followerTask = pipeline.AppendFollowerAsync(replicaIndex, last, cancellationToken).AsTask();
            if (Array.IndexOf(startedTasks, followerTask, 0, startedCount) >= 0)
            {
                pipeline.RecordLaggingReplica(replicaIndex, last.LogIndex);
                continue;
            }

            startedTasks[startedCount] = followerTask;
            pending.Add(
                run.Length == 1
                    ? ReplicaFollowerObservation.AwaitFollowerAsync(replicaIndex, followerTask)
                    : ObserveRunAsync(replicaIndex, quorum, pipeline, run, EntryTasks(replicaIndex, pipeline, run, followerTask, cancellationToken)));
            startedCount++;
        }
    }

    /// <summary>Records a follower completion of the run unless its acknowledgements were already recorded as they completed.</summary>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="pipeline">Pipeline that appends to followers and records lagging replicas.</param>
    /// <param name="follower">The follower completion, of one entry or of the whole run.</param>
    /// <param name="run">The entries being committed, in log order.</param>
    internal static void RecordCompletion(ReplicaCommitQuorum quorum, IReplicaCommitPipeline pipeline, in FollowerCompletion follower, PreparedReplicaMutation[] run)
    {
        if (!follower.Recorded)
            RecordAcknowledgement(quorum, pipeline, in follower, run[^1]);
    }

    /// <summary>Records follower completions of the entry until a majority backs it, no follower or progress that could complete it is left, or the token ends.</summary>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="pipeline">Pipeline that records lagging replicas.</param>
    /// <param name="pending">The follower observations of the run that did not complete yet.</param>
    /// <param name="run">The entries being committed, in log order; the majority is awaited on the last one.</param>
    /// <param name="commitIndex">The group commit index, which does not change while the caller holds the commit gate.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    /// <returns>An asynchronous operation; the caller checks whether the majority was reached.</returns>
    /// <remarks>Call only under the coordinator's commit gate, so <paramref name="commitIndex" /> cannot go stale.</remarks>
    internal static async Task AwaitMajorityAsync(
        ReplicaCommitQuorum quorum,
        IReplicaCommitPipeline pipeline,
        List<Task<FollowerCompletion>> pending,
        PreparedReplicaMutation[] run,
        ulong commitIndex,
        CancellationToken cancellationToken)
    {
        var lastIndex = run[^1].LogIndex;

        // Rebuilt only once a follower completion is consumed, so wakes from other paths do not stack continuations on every follower task.
        var next = pending.Count > 0 ? Task.WhenAny(pending) : null;
        while (true)
        {
            // Read before the majority check, so an acknowledgement recorded after the check wakes the wait below.
            var progressVersion = quorum.ProgressVersion;
            if (quorum.FindCommitIndex(commitIndex, lastIndex) >= lastIndex)
                return;

            if (next == null)
            {
                // No follower of this entry is left to answer, but an acknowledgement buffered behind a prefix that an earlier entry's
                // observation still records can complete the majority: wait for that progress, within the budget, before giving up.
                if (!quorum.HasBufferedThrough(lastIndex))
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
            RecordCompletion(quorum, pipeline, in follower, run);
            next = pending.Count > 0 ? Task.WhenAny(pending) : null;
        }
    }

    private static Task<ReplicaDurableAcknowledgement>[] EntryTasks(
        int replicaIndex,
        IReplicaCommitPipeline pipeline,
        PreparedReplicaMutation[] run,
        Task<ReplicaDurableAcknowledgement> lastTask,
        CancellationToken cancellationToken)
    {
        // Taken at once: the pipeline serves only its last run, and the observation outlives the commit that appended it.
        var entryTasks = new Task<ReplicaDurableAcknowledgement>[run.Length];
        for (var i = 0; i < entryTasks.Length - 1; i++)
            entryTasks[i] = pipeline.AppendFollowerAsync(replicaIndex, run[i], cancellationToken).AsTask();

        entryTasks[^1] = lastTask;
        return entryTasks;
    }

    /// <summary>Takes the append tasks of one follower for the entries of a run in log order and records each acknowledgement as its task completes.</summary>
    /// <param name="replicaIndex">Zero-based replica slot of the follower.</param>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="pipeline">Pipeline that appends to followers and records lagging replicas.</param>
    /// <param name="run">The entries being committed, in log order.</param>
    /// <param name="entryTasks">The follower's task of each entry, in log order, taken while the run is still the pipeline's last one.</param>
    /// <returns>A completion that has everything recorded; it completes after the last entry.</returns>
    /// <remarks>
    /// A result is read only from a completed task: the tasks are taken in log order, and on the normal path a sender completes the
    /// entries of one request in order, but a disposed sender fails its waiting entries before the request in flight ends. The first
    /// entry the follower did not acknowledge marks the slot lagging once. An acknowledgement past it is still recorded; the quorum
    /// buffers it until the prefix lands. Recording as each entry completes lets a majority of a prefix show before the last entry
    /// is answered.
    /// </remarks>
    private static async Task<FollowerCompletion> ObserveRunAsync(
        int replicaIndex,
        ReplicaCommitQuorum quorum,
        IReplicaCommitPipeline pipeline,
        PreparedReplicaMutation[] run,
        Task<ReplicaDurableAcknowledgement>[] entryTasks)
    {
        var lagging = false;
        for (var i = 0; i < run.Length; i++)
        {
            // Awaited as a plain task without rethrowing, so the wait builds no combinator and does not surface the fault of the append.
            var task = entryTasks[i];
            Task completion = task;
            await completion.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            if (task.IsCompletedSuccessfully)
            {
                var acknowledgement = await task.ConfigureAwait(false);
                if (quorum.TryRecord(replicaIndex, in acknowledgement, run[i]))
                    continue;
            }
            else
            {
                _ = task.Exception;
            }

            if (lagging)
                continue;

            pipeline.RecordLaggingReplica(replicaIndex, run[i].LogIndex);
            lagging = true;
        }

        return new FollowerCompletion(replicaIndex, null, true);
    }

    /// <summary>Records a follower completion, or marks the replica lagging when it did not acknowledge.</summary>
    /// <param name="quorum">Quorum that records follower acknowledgements.</param>
    /// <param name="pipeline">Pipeline that appends to followers and records lagging replicas.</param>
    /// <param name="follower">The follower completion.</param>
    /// <param name="mutation">The entry being committed.</param>
    private static void RecordAcknowledgement(ReplicaCommitQuorum quorum, IReplicaCommitPipeline pipeline, in FollowerCompletion follower, PreparedReplicaMutation mutation)
    {
        if (follower.Acknowledgement is { } acknowledgement && quorum.TryRecord(follower.ReplicaIndex, in acknowledgement, mutation))
            return;

        pipeline.RecordLaggingReplica(follower.ReplicaIndex, mutation.LogIndex);
    }
}
