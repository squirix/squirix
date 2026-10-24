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
    private readonly ReplicaRecoveredTail? _recovered;
    private readonly Action<PreparedReplicaMutation> _resolved;
    private readonly Lock _sync = new();
    private int _activeApplies;
    private int _activeExecutions;
    private bool _closed;

    /// <summary>Initializes a new instance of the <see cref="ReplicaPendingApplies" /> class.</summary>
    /// <param name="pipeline">Pipeline that applies committed entries to memory.</param>
    /// <param name="idempotency">Group idempotency state that resolves re-applied entries.</param>
    /// <param name="resolved">Called after a re-applied entry resolved its idempotency record.</param>
    /// <param name="recovered">
    /// Uncommitted tail recovered at start, retained with an unresolved idempotency pin per entry, or <see langword="null" /> when the
    /// log tail is fully committed.
    /// </param>
    /// <exception cref="InvalidOperationException">A recovered entry cannot be pinned: its identity is retained with another fingerprint.</exception>
    internal ReplicaPendingApplies(
        IReplicaCommitPipeline pipeline,
        GroupIdempotencyState idempotency,
        Action<PreparedReplicaMutation> resolved,
        ReplicaRecoveredTail? recovered = null)
    {
        ArgumentNullException.ThrowIfNull(pipeline);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(resolved);
        _pipeline = pipeline;
        _idempotency = idempotency;
        _resolved = resolved;
        _recovered = recovered;
        if (recovered == null)
            return;

        // A same-identity retry must find the recovered entry pinned, never re-execute it: its outcome is unknown until a commit covers it.
        foreach (var entry in recovered.Mutations)
        {
            var kind = GroupRecordKinds.FromScope(entry.OperationScope);
            var reserved = idempotency.Reserve(entry.OperationScope, entry.OperationId, entry.OperationFingerprint.Span, kind, entry.LogIndex, entry.Term, new GroupReserveOptions(true, entry.OutcomePayload.Length));
            if (reserved != GroupIdempotencyReserveResult.Success)
                throw new InvalidOperationException($"Recovered log entry {entry.LogIndex} cannot be pinned for idempotent retries: {reserved}.");

            _entries[entry.LogIndex] = entry;
        }
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

    /// <summary>Applies every retained entry at or below <paramref name="commitIndex" /> in log order, all of them foreign.</summary>
    /// <param name="commitIndex">Durable group commit index.</param>
    /// <returns>An asynchronous operation.</returns>
    internal Task ApplyThroughAsync(ulong commitIndex) => ApplyThroughAsync(commitIndex, 1, 0, false);

    /// <summary>Applies every retained entry at or below <paramref name="commitIndex" /> in log order.</summary>
    /// <param name="commitIndex">Durable group commit index.</param>
    /// <param name="ownFirst">First log index of the entries of the commit running this apply; above <paramref name="ownLast" /> when no caller owns any entry.</param>
    /// <param name="ownLast">Last log index of the entries of the commit running this apply.</param>
    /// <param name="resolveOwn">
    /// Whether the entries of the commit are those of a run: each resolves its idempotency record right after it is applied, and is applied
    /// without the ambient scope of the caller, like a foreign entry (which always does both). A single own entry is applied inline in the
    /// context of the caller and resolves once its commit returns.
    /// </param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// Runs on <see cref="CancellationToken.None" />: every entry is past its durable majority. A failure leaves the failed entry and
    /// every later one retained, so the next apply resumes in order; the entries before it stay applied and, when they are resolved
    /// here, resolved.
    /// </remarks>
    internal async Task ApplyThroughAsync(ulong commitIndex, ulong ownFirst, ulong ownLast, bool resolveOwn)
    {
        while (TryPeekDue(commitIndex, out var pending))
        {
            var foreign = pending.LogIndex < ownFirst || pending.LogIndex > ownLast;

            // The outcome is the one decided at prepare time, for a recovered entry too: its record carries it.
            var outcome = pending.OutcomePayload;

            // An entry of a run is applied without the ambient scope of the caller, like a foreign one: that scope would otherwise
            // stamp the cache-WAL frames of every entry of the run with the id of one caller.
            if (foreign || resolveOwn)
                await ApplyForeignAsync(pending).ConfigureAwait(false);
            else
                await _pipeline.ApplyMemoryAsync(pending, CancellationToken.None).ConfigureAwait(false);

            lock (_sync)
                _ = _entries.Remove(pending.LogIndex);

            // A re-applied entry's own commit already reported an unknown outcome and kept both pins. It is now committed and applied,
            // so resolve them: a same-identity retry replays the outcome instead of staying unknown until the record ages out.
            // An own entry of a run drops its operation even when its record is gone, as nothing is left to wait for.
            var resolved = (foreign || resolveOwn) && _idempotency.TryResolve(pending.OperationScope, pending.OperationId, outcome.Span, pending.LogIndex, pending.Term);
            if (resolved || (resolveOwn && !foreign))
                _resolved(pending);
        }
    }

    /// <summary>Refuses every later drive of the committed entries.</summary>
    /// <returns><see langword="true" /> when a drive or a commit body that started earlier is still running.</returns>
    internal bool CloseForApplies()
    {
        lock (_sync)
        {
            _closed = true;
            return _activeApplies > 0 || _activeExecutions > 0;
        }
    }

    /// <summary>Clamps a majority-backed commit candidate to the current-term commit rule of the recovered tail.</summary>
    /// <param name="commitIndex">Current durable group commit index.</param>
    /// <param name="candidate">Highest index a recorded majority backs.</param>
    /// <returns><paramref name="candidate" /> when it may be committed; otherwise <paramref name="commitIndex" />.</returns>
    /// <remarks>
    /// Recovered entries of an older term are not committed by counting replicas: they wait until a current-term entry above them
    /// commits them transitively.
    /// </remarks>
    internal ulong CommittableIndex(ulong commitIndex, ulong candidate) =>
        candidate > commitIndex && _recovered?.CanCommitThrough(candidate) == false ? commitIndex : candidate;

    /// <summary>Determines whether some retained entry is at or below <paramref name="commitIndex" />.</summary>
    /// <param name="commitIndex">Commit index a majority backs.</param>
    /// <returns><see langword="true" /> when that commit index covers a retained entry.</returns>
    internal bool Covers(ulong commitIndex)
    {
        lock (_sync)
            return _entries.Count > 0 && _entries.Keys[0] <= commitIndex;
    }

    /// <summary>Counts a commit body that starts running.</summary>
    /// <remarks>
    /// The operations of a run leave the table entry by entry as they are applied, before the body ends, so the table cannot tell whether
    /// the body still holds the gates.
    /// </remarks>
    internal void EnterExecution()
    {
        lock (_sync)
            _activeExecutions++;
    }

    /// <summary>Counts a commit body that stopped running.</summary>
    internal void ExitExecution()
    {
        lock (_sync)
            _activeExecutions--;
    }

    /// <summary>Counts a drive of the committed entries that stopped running.</summary>
    internal void EndApply()
    {
        lock (_sync)
            _activeApplies--;
    }

    /// <summary>Retains a locally appended entry until a commit covers it.</summary>
    /// <param name="mutation">The locally appended mutation.</param>
    internal void Retain(PreparedReplicaMutation mutation)
    {
        lock (_sync)
            _entries[mutation.LogIndex] = mutation;
    }

    /// <summary>Counts a drive of the committed entries that starts running, unless applies are closed.</summary>
    /// <returns><see langword="false" /> when <see cref="CloseForApplies" /> already ran and no drive may start.</returns>
    internal bool TryBeginApply()
    {
        lock (_sync)
        {
            if (_closed)
                return false;

            _activeApplies++;
            return true;
        }
    }

    /// <summary>Applies an entry whose outcome belongs to no caller of the current execution.</summary>
    /// <param name="entry">The retained entry.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// The current execution context belongs to another operation (the commit that re-applies the entry, or the commit whose
    /// background follower observation resolved it) and can carry that RPC's idempotency scope, which would stamp the entry's
    /// cache-WAL frame with the foreign operation id and count it as that RPC's effect. Starting the apply on a
    /// pool thread without flowing the context runs it with no ambient scope at all. The apply marks itself as replicated on its
    /// own, so it skips the wait for the node journal flush even without a scope.
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
        static async state =>
        {
            if (state is not ForeignApply apply)
                throw new InvalidOperationException("The foreign apply was started without its state.");

            await apply.Pipeline.ApplyMemoryAsync(apply.Entry, CancellationToken.None).ConfigureAwait(false);
        },
        new ForeignApply(_pipeline, entry),
        CancellationToken.None,
        TaskCreationOptions.DenyChildAttach,
        TaskScheduler.Default).Unwrap();

    private bool TryPeekDue(ulong commitIndex, [NotNullWhen(true)] out PreparedReplicaMutation? pending)
    {
        lock (_sync)
        {
            pending = _entries.Count > 0 && _entries.Keys[0] <= commitIndex ? _entries.Values[0] : null;
            return pending != null;
        }
    }

    [Immutable]
    private sealed record ForeignApply(IReplicaCommitPipeline Pipeline, PreparedReplicaMutation Entry);
}
