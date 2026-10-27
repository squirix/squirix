using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The port through which a state transfer reaches the cache of the node: it wipes the keys of a group and writes the transferred entries.</summary>
/// <remarks>
/// The cluster namespace cannot reach the cache, so the node services implement this port. A transfer is entered once, wiped in bounded steps,
/// written in batches, sealed, and left; the calls of one group never overlap.
/// </remarks>
internal interface IReplicaStateSink
{
    /// <summary>Gets a value indicating whether the node journal is at its high-water mark, where a transfer pauses and leaves the headroom to client writes and the follower apply.</summary>
    bool IsJournalBusy { get; }

    /// <summary>Gets a value indicating whether the snapshot service failed for good, so the journal is never freed and waiting for it is pointless.</summary>
    bool IsSnapshotFailed { get; }

    /// <summary>Waits for the local journal recovery, then takes the apply driver of the group so no apply or maintenance pass runs for it.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> once the group is entered; <see langword="false" /> when this node leads the group, which a transfer never repairs.</returns>
    ValueTask<bool> EnterAsync(string groupId, CancellationToken cancellationToken);

    /// <summary>Removes the keys of the group, examining at most a bounded number of them.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="maxExamined">The largest number of keys of the group to examine and remove in this call.</param>
    /// <param name="cancellationToken">Cancellation token; checked between keys only, so no examined key is skipped.</param>
    /// <returns>
    /// <see cref="ReplicaWipeStep.More" /> while the group may still have keys; <see cref="ReplicaWipeStep.Done" /> once it has none;
    /// <see cref="ReplicaWipeStep.JournalBusy" /> when the node journal refused a removal, the key being kept for the next step.
    /// </returns>
    ValueTask<ReplicaWipeStep> WipeStepAsync(string groupId, int maxExamined, CancellationToken cancellationToken);

    /// <summary>Writes entries to the cache, each with its absolute deadline, whether or not the deadline has passed.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="entries">The entries, in arrival order.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The number of entries written: all of them, or fewer when the node journal refused the next one, which is then not written and changes nothing.</returns>
    /// <exception cref="System.IO.InvalidDataException">An entry does not decode or names a deadline out of range; entries before it are written.</exception>
    ValueTask<int> WriteAsync(string groupId, ReadOnlyMemory<ReplicaStateEntry> entries, CancellationToken cancellationToken);

    /// <summary>Awaits the journal durability barrier over everything written since the group was entered, then reports what was written.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The count and the CRC32C of the written entries.</returns>
    ValueTask<ReplicaStateTotals> SealAsync(string groupId, CancellationToken cancellationToken);

    /// <summary>Sets the applied index of the group applier to the index the transferred state is taken at, without logging a gap.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="index">The applied index.</param>
    void ResetAppliedTo(string groupId, ulong index);

    /// <summary>Releases the apply driver of the group and drops what the transfer kept; safe to call from a timer callback.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    void Leave(string groupId);
}
