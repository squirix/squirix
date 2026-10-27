using System;
using System.Buffers;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Writes a state transfer into the cache of the node: wipes the keys of one group and stores the transferred entries.</summary>
/// <remarks>
/// <para>
/// Every key is removed and every entry stored through the local cache chain the follower apply uses, below memory admission and with the
/// operation stamping suspended, so the effects reach memory and the cache journal like applied records do. An entry already past its deadline is
/// stored all the same: on a replicated host expiry is decided by committed records, and the replayed record removes it.
/// </para>
/// <para>
/// A wipe step examines a bounded number of keys of the group. The walk over the node entries is kept between the steps of a group, so a large group
/// is wiped in many short calls without walking the other groups again. Nothing writes the keys of a group meanwhile: the apply driver of the group
/// is held from <see cref="EnterAsync" /> to <see cref="Leave" />.
/// </para>
/// <para>
/// The transfer is the lowest-priority writer of the node journal: before every key it checks the high-water mark of the journal and stops there,
/// leaving the headroom above it to client writes and the follower apply. A refusal of the journal quota that slips through is caught as well; a
/// refused write changes neither memory nor journal, so the same key or entry is simply tried again by the next step.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaStateSink : IReplicaStateSink
{
    private readonly ReplicaGroupAppliers _appliers;
    private readonly ReplicaGroupCommitters _committers;
    private readonly ReplicaGroupEntries _entries;
    private readonly IJournalCoordinator _journal;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly ISnapshotReadinessStatus _snapshots;
    private readonly FrozenDictionary<string, GroupWork> _work;

    /// <summary>Initializes a new instance of the <see cref="ReplicaStateSink" /> class.</summary>
    /// <param name="appliers">The appliers of the served groups, whose driver lease the sink takes and whose applied index it reseeds.</param>
    /// <param name="committers">The committers of the led groups; a group this node leads is never entered.</param>
    /// <param name="local">The local cache chain the follower apply writes through.</param>
    /// <param name="entries">The walk of the node entries by group.</param>
    /// <param name="journal">
    /// The node cache journal: its startup gate opens once local recovery has replayed the journal into memory, its durability barrier seals the transfer, and its
    /// high-water mark pauses it.
    /// </param>
    /// <param name="snapshots">The status of the snapshot service, which frees the journal.</param>
    internal ReplicaStateSink(
        ReplicaGroupAppliers appliers,
        ReplicaGroupCommitters committers,
        ILogicalNamespacedCache<object?> local,
        ReplicaGroupEntries entries,
        IJournalCoordinator journal,
        ISnapshotReadinessStatus snapshots)
    {
        ArgumentNullException.ThrowIfNull(appliers);
        ArgumentNullException.ThrowIfNull(committers);
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(journal);
        ArgumentNullException.ThrowIfNull(snapshots);
        _appliers = appliers;
        _committers = committers;
        _local = local;
        _entries = entries;
        _journal = journal;
        _snapshots = snapshots;
        var work = new Dictionary<string, GroupWork>(appliers.GroupIds.Count, StringComparer.Ordinal);
        for (var i = 0; i < appliers.GroupIds.Count; i++)
            work.Add(appliers.GroupIds[i], new GroupWork());

        _work = work.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public bool IsJournalBusy => _journal.AtHighWater;

    /// <inheritdoc />
    public bool IsSnapshotFailed => _snapshots.HasFatalFailure;

    /// <inheritdoc />
    /// <remarks>Waits for local recovery first: a journal replay that ran after the wipe could insert keys the wipe removed.</remarks>
    public async ValueTask<bool> EnterAsync(string groupId, CancellationToken cancellationToken)
    {
        await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);
        if (_committers.Leads(groupId) || !await _appliers.For(groupId).DriverLease.TryLeadAsync(cancellationToken).ConfigureAwait(false))
            return false;

        _work[groupId].Reset();
        return true;
    }

    /// <inheritdoc />
    public void Leave(string groupId)
    {
        _work[groupId].Reset();
        _appliers.For(groupId).DriverLease.EndLeading();
    }

    /// <inheritdoc />
    public void ResetAppliedTo(string groupId, ulong index) => _appliers.For(groupId).ResetTo(index);

    /// <inheritdoc />
    public async ValueTask<ReplicaStateTotals> SealAsync(string groupId, CancellationToken cancellationToken)
    {
        await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        return _work[groupId].Checksum.Totals;
    }

    /// <inheritdoc />
    /// <remarks>
    /// The token is checked before a key is taken from the walk, never between taking it and removing it. A key the journal refused to remove stays pending and is
    /// removed first by the next step, so no examined key is skipped.
    /// </remarks>
    public async ValueTask<ReplicaWipeStep> WipeStepAsync(string groupId, int maxExamined, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxExamined);
        var work = _work[groupId];
        var cursor = work.Cursor ??= _entries.EnumerateAsync(groupId, CancellationToken.None).GetAsyncEnumerator(CancellationToken.None);
        for (var examined = 0; examined < maxExamined; examined++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (work.PendingKey is not { } pending)
            {
                if (!await cursor.MoveNextAsync().ConfigureAwait(false))
                {
                    work.Cursor = null;
                    return ReplicaWipeStep.Done;
                }

                pending = cursor.Current;
                work.PendingKey = pending;
            }

            if (_journal.AtHighWater || !await TryRemoveAsync(pending).ConfigureAwait(false))
                return ReplicaWipeStep.JournalBusy;

            work.PendingKey = null;
        }

        return ReplicaWipeStep.More;
    }

    /// <inheritdoc />
    /// <exception cref="InvalidDataException">An entry belongs to another group, or does not decode; nothing of the batch is written for an entry of another group.</exception>
    public async ValueTask<int> WriteAsync(string groupId, ReadOnlyMemory<ReplicaStateEntry> entries, CancellationToken cancellationToken)
    {
        var work = _work[groupId];
        var keys = ArrayPool<string>.Shared.Rent(entries.Length);
        try
        {
            EnsureOwnedBy(groupId, entries.Span, keys);

            // The stamping is suspended once for the whole batch: each suspension allocates a frame, and the batch writes every entry the same way.
            using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
            {
                for (var i = 0; i < entries.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (_journal.AtHighWater)
                        return i;

                    var item = entries.Span[i];
                    var entry = Decode(in item, out var encodedLength);
                    try
                    {
                        await _local.SetEntryAsync(string.Empty, item.CacheName, keys[i], entry, encodedLength, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (JournalCapacityExceededException)
                    {
                        // The journal refused the frame before it reached the ring: neither the journal nor memory changed, so the next step writes the entry.
                        return i;
                    }

                    work.Checksum.Add(in item);
                }
            }

            return entries.Length;
        }
        finally
        {
            ArrayPool<string>.Shared.Return(keys, true);
        }
    }

    /// <summary>Decodes the cache entry a transferred entry carries, with its absolute deadline.</summary>
    /// <param name="item">The transferred entry.</param>
    /// <param name="encodedLength">The journal-encoded length of the entry, or <see langword="null" /> when it is not known.</param>
    /// <returns>The entry to store.</returns>
    /// <exception cref="InvalidDataException">The payload does not decode or the deadline is out of range.</exception>
    private static NodeCacheEntry<object?> Decode(in ReplicaStateEntry item, out int? encodedLength)
    {
        if (!WireBounds.IsUtcTicks(item.ExpiresUtcTicks))
            throw new InvalidDataException("A transferred entry names a deadline out of range.");

        var decoded = JournalEntryPayload.TryDecodeWithDeadline(item.Payload.Span, item.ExpiresUtcTicks, out var entry, out encodedLength);
        return decoded && entry != null
            ? entry
            : throw new InvalidDataException("A transferred entry payload is undecodable.");
    }

    /// <summary>Refuses a batch with an entry the locator does not assign to the group of the transfer, before anything is written.</summary>
    /// <param name="groupId">The group of the transfer.</param>
    /// <param name="entries">The batch.</param>
    /// <param name="keys">Receives the key of every entry as a string, in the order of the batch; it holds at least as many slots as the batch has entries.</param>
    /// <exception cref="InvalidDataException">An entry belongs to another group.</exception>
    private void EnsureOwnedBy(string groupId, ReadOnlySpan<ReplicaStateEntry> entries, string[] keys)
    {
        for (var i = 0; i < entries.Length; i++)
        {
            var key = Encoding.UTF8.GetString(entries[i].Key.Span);
            var owner = _entries.OwnerOf(entries[i].CacheName, key);
            if (!string.Equals(owner, groupId, StringComparison.Ordinal))
                throw new InvalidDataException("A transferred entry belongs to another group.");

            keys[i] = key;
        }
    }

    /// <summary>Removes one key; a refusal of the journal quota leaves the key in place.</summary>
    /// <param name="current">The key to remove.</param>
    /// <returns><see langword="true" /> when the key is removed; <see langword="false" /> when the journal refused the removal.</returns>
    private async ValueTask<bool> TryRemoveAsync(ReplicaGroupEntry current)
    {
        using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
        {
            try
            {
                _ = await _local.RemoveAsync(string.Empty, current.CacheName, current.Key, CancellationToken.None).ConfigureAwait(false);
                return true;
            }
            catch (JournalCapacityExceededException)
            {
                return false;
            }
        }
    }

    /// <summary>What the sink keeps for one group between the calls of a transfer.</summary>
    private sealed class GroupWork
    {
        /// <summary>Gets the running checksum of the entries written.</summary>
        internal ReplicaStateChecksum Checksum { get; } = new();

        /// <summary>Gets or sets the walk over the entries of the group; <see langword="null" /> before the first wipe step and after the last.</summary>
        /// <remarks>An iterator over the entries holds nothing that needs disposal, so dropping it on abort leaks nothing.</remarks>
        internal IAsyncEnumerator<ReplicaGroupEntry>? Cursor { get; set; }

        /// <summary>Gets or sets the key taken from the walk whose removal the journal refused; the next wipe step removes it first.</summary>
        internal ReplicaGroupEntry? PendingKey { get; set; }

        /// <summary>Forgets the walk, the pending key and the checksum.</summary>
        internal void Reset()
        {
            Cursor = null;
            PendingKey = null;
            Checksum.Reset();
        }
    }
}
