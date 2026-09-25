using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Locally appended entries retained until a commit covers them, then applied to memory in log order.</summary>
/// <remarks>
/// Entries are added and applied only under the owning coordinator's commit gate, which keeps the apply order exact.
/// The emptiness and coverage probes may run from any thread (the committer's pre-prepare check and background follower
/// observation), so every access to the entries is synchronized.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaPendingApplies
{
    private readonly SortedList<ulong, PreparedReplicaMutation> _entries = [];
    private readonly GroupIdempotencyState _idempotency;
    private readonly IReplicaCommitPipeline _pipeline;
    private readonly Action<PreparedReplicaMutation> _resolved;
    private readonly Lock _sync = new();

    /// <summary>Initializes a new instance of the <see cref="ReplicaPendingApplies" /> class.</summary>
    /// <param name="pipeline">Pipeline that applies committed entries to memory.</param>
    /// <param name="idempotency">Group idempotency state that resolves re-applied entries.</param>
    /// <param name="resolved">Called after a re-applied entry resolved its idempotency record.</param>
    internal ReplicaPendingApplies(IReplicaCommitPipeline pipeline, GroupIdempotencyState idempotency, Action<PreparedReplicaMutation> resolved)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(resolved);
        _pipeline = pipeline;
        _idempotency = idempotency;
        _resolved = resolved;
    }

    /// <summary>Gets a value indicating whether every locally appended entry is applied.</summary>
    internal bool IsEmpty
    {
        get
        {
            lock (_sync)
                return _entries.Count == 0;
        }
    }

    /// <summary>Gets the highest retained log index, or zero when nothing is retained.</summary>
    internal ulong LastIndex
    {
        get
        {
            lock (_sync)
                return _entries.Count == 0 ? 0 : _entries.Keys[^1];
        }
    }

    /// <summary>Applies every retained entry at or below <paramref name="commitIndex" /> in log order.</summary>
    /// <param name="commitIndex">Durable group commit index.</param>
    /// <param name="own">The mutation of the commit running this apply, or <see langword="null" /> when no caller owns any entry.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// Runs on <see cref="CancellationToken.None" />: every entry is past its durable majority. A failure leaves the failed entry and
    /// every later one retained, so the next apply resumes in order.
    /// </remarks>
    internal async Task ApplyThroughAsync(ulong commitIndex, PreparedReplicaMutation? own)
    {
        while (TryPeekDue(commitIndex, out var pending))
        {
            var foreign = !ReferenceEquals(pending, own);
            if (foreign)
                await ApplyForeignAsync(pending).ConfigureAwait(false);
            else
                await _pipeline.ApplyMemoryAsync(pending, CancellationToken.None).ConfigureAwait(false);

            lock (_sync)
                _ = _entries.Remove(pending.LogIndex);

            // A re-applied entry's own commit already reported an unknown outcome and kept both pins. It is now committed and applied,
            // so resolve them: a same-identity retry replays the outcome instead of staying unknown until the record ages out.
            if (foreign && _idempotency.TryResolve(pending.OperationScope, pending.OperationId, pending.OutcomePayload.Span, pending.LogIndex, pending.Term))
                _resolved(pending);
        }
    }

    /// <summary>Determines whether some retained entry is at or below <paramref name="commitIndex" />.</summary>
    /// <param name="commitIndex">Commit index a majority backs.</param>
    /// <returns><see langword="true" /> when that commit index covers a retained entry.</returns>
    internal bool Covers(ulong commitIndex)
    {
        lock (_sync)
            return _entries.Count > 0 && _entries.Keys[0] <= commitIndex;
    }

    /// <summary>Retains a locally appended entry until a commit covers it.</summary>
    /// <param name="mutation">The locally appended mutation.</param>
    internal void Retain(PreparedReplicaMutation mutation)
    {
        lock (_sync)
            _entries[mutation.LogIndex] = mutation;
    }

    /// <summary>Applies an entry whose outcome belongs to no caller of the current execution.</summary>
    /// <param name="entry">The retained entry.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// The current execution context belongs to another operation (the commit that re-applies the entry, or the commit whose
    /// background follower observation resolved it) and can carry that RPC's idempotency scope, which would stamp the entry's
    /// cache-WAL frame with the foreign operation id and defer its durability to that RPC's outcome. Starting the apply on a
    /// pool thread without flowing the context runs it with no ambient scope at all.
    /// </remarks>
    private Task ApplyForeignAsync(PreparedReplicaMutation entry)
    {
        // The flow suppression covers only the start, and must be undone on this thread before the caller awaits.
        Task apply;
        if (ExecutionContext.IsFlowSuppressed())
        {
            apply = StartApplyAsync(entry);
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
                apply = StartApplyAsync(entry);
        }

        return apply;
    }

    private Task StartApplyAsync(PreparedReplicaMutation entry) => Task.Factory.StartNew(
            async () => await _pipeline.ApplyMemoryAsync(entry, CancellationToken.None).ConfigureAwait(false),
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default)
        .Unwrap();

    private bool TryPeekDue(ulong commitIndex, [NotNullWhen(true)] out PreparedReplicaMutation? pending)
    {
        lock (_sync)
        {
            pending = _entries.Count > 0 && _entries.Keys[0] <= commitIndex ? _entries.Values[0] : null;
            return pending != null;
        }
    }
}
