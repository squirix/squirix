using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Applies the committed entries of the owned replica group to local memory, densely in log order, and tracks how far they are applied.</summary>
/// <remarks>
/// The committer owns one instance for its whole lifetime, so the applied index survives resyncs and coordinator replacement. Every
/// apply path goes through <see cref="ApplyAsync" />: the commits of the running coordinator (own and late-majority entries alike) and
/// the re-apply of committed entries at start. The callers serialize the applies (the coordinator's commit gate, or the committer
/// gate while no coordinator runs); the applied index is published with volatile writes because the applied-index flush reads it
/// outside both gates.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaLeaderApplier
{
    private readonly ILogicalNamespacedCache<object?> _local;
    private ulong _appliedIndex;

    /// <summary>Whether the applied index was seeded from the durable one; read and written under the committer gate only.</summary>
    private bool _seeded;

    /// <summary>Initializes a new instance of the <see cref="ReplicaLeaderApplier" /> class.</summary>
    /// <param name="local">Local cache pipeline the entries are applied to.</param>
    internal ReplicaLeaderApplier(ILogicalNamespacedCache<object?> local)
    {
        ArgumentNullException.ThrowIfNull(local);
        _local = local;
    }

    /// <summary>Gets the highest log index applied to memory: the seeded durable applied index, then the last entry applied.</summary>
    /// <remarks>Every entry at or below it has returned from its apply, so its cache journal frame is appended.</remarks>
    internal ulong AppliedIndex => Volatile.Read(ref _appliedIndex);

    /// <summary>Applies the next committed entry to memory and advances the applied index to it.</summary>
    /// <param name="logIndex">The log index of the entry.</param>
    /// <param name="canonicalPayload">The canonical record bytes of the entry.</param>
    /// <param name="clock">Time source measuring the pinned expiration deadlines.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The entry is not the one after the applied index, or its payload does not decode.</exception>
    /// <remarks>The order check runs before anything is applied, so an entry out of order never reaches memory.</remarks>
    internal async ValueTask ApplyAsync(ulong logIndex, ReadOnlyMemory<byte> canonicalPayload, TimeProvider clock, CancellationToken cancellationToken)
    {
        var next = AppliedIndex + 1;
        if (logIndex != next)
            throw new InvalidOperationException($"Replica log entry {logIndex} cannot be applied: the next entry to apply is {next}.");

        var record = ReplicaLogCodec.Decode(canonicalPayload) ??
            ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException($"Replica log entry {logIndex} carries an undecodable canonical payload."));
        _ = await ReplicaCacheApplier.ApplyAsync(_local, record, clock, cancellationToken).ConfigureAwait(false);
        Volatile.Write(ref _appliedIndex, logIndex);
    }

    /// <summary>Applies the committed entries memory may lack, so it holds every entry through <paramref name="commitIndex" />.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="durableAppliedIndex">The durable applied index of <paramref name="log" />, which seeds the applied index on the first call.</param>
    /// <param name="commitIndex">The durable commit index of <paramref name="log" />.</param>
    /// <param name="clock">Time source measuring the pinned expiration deadlines.</param>
    /// <param name="cancellationToken">Cancellation token for reading the log; the applies themselves are not canceled.</param>
    /// <returns>A task that completes when the applied index reaches <paramref name="commitIndex" />.</returns>
    /// <exception cref="InvalidOperationException">The retained committed entries do not reach <paramref name="commitIndex" /> densely.</exception>
    /// <remarks>
    /// Called under the committer gate, while no coordinator runs. After a restart memory holds at most what the cache journal kept,
    /// which may miss the entries above the durable applied index: they are applied again, in log order. The applied index is seeded
    /// only once, so a resync in this process keeps the in-memory index and applies nothing twice. The entries belong to no caller of
    /// the current execution, which can carry the idempotency scope of the write that started the committer; that scope would stamp their
    /// cache journal frames with a foreign operation, so they are applied on a pool thread started without the execution context.
    /// </remarks>
    internal async Task CatchUpAsync(IFollowerLog log, ulong durableAppliedIndex, ulong commitIndex, TimeProvider clock, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(clock);
        if (!_seeded)
        {
            Volatile.Write(ref _appliedIndex, durableAppliedIndex);
            _seeded = true;
        }

        if (AppliedIndex >= commitIndex)
            return;

        var committed = await log.GetCommittedEntriesAsync(cancellationToken).ConfigureAwait(false);

        // The flow suppression covers only the start, and must be undone on this thread before the apply is awaited.
        Task reapply;
        if (ExecutionContext.IsFlowSuppressed())
        {
            reapply = StartReapplyAsync(committed, commitIndex, clock);
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
                reapply = StartReapplyAsync(committed, commitIndex, clock);
        }

        await reapply.ConfigureAwait(false);
    }

    private async Task ReapplyCoreAsync(IReadOnlyList<FollowerLogEntry> committed, ulong commitIndex, TimeProvider clock)
    {
        foreach (var entry in committed)
        {
            if (entry.LogIndex > commitIndex)
                break;

            if (entry.LogIndex > AppliedIndex)
                await ApplyAsync(entry.LogIndex, entry.Payload, clock, CancellationToken.None).ConfigureAwait(false);
        }

        if (AppliedIndex < commitIndex)
            throw new InvalidOperationException($"The owned group log retains no entry {AppliedIndex + 1} to apply through its commit index {commitIndex}.");
    }

    private Task StartReapplyAsync(IReadOnlyList<FollowerLogEntry> committed, ulong commitIndex, TimeProvider clock) => Task.Factory.StartNew(
            () => ReapplyCoreAsync(committed, commitIndex, clock),
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default)
        .Unwrap();
}
