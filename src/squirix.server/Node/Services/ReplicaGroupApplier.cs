using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Applies the committed entries of one replica group to local memory, densely in log order, and tracks how far they are applied.</summary>
/// <remarks>
/// One instance serves one group for its whole lifetime, so the applied index survives resyncs and coordinator replacement. The leader
/// committer drives the instance of a group this node leads, and the one loop of a follower group drives the instance of that group. Every
/// apply path goes through <see cref="ApplyAsync" />: the commits of the running coordinator (own and late-majority entries alike) and
/// the re-apply of committed entries at start. The callers serialize the applies (the coordinator's commit gate, or the committer
/// gate while no coordinator runs, or the one loop of a follower group); the applied index is published with volatile writes because the applied-index flush reads it
/// outside both gates. An apply executes the effect its record carries and reads no clock. A record that is inconsistent, or cannot
/// be decoded, is never applied: it is logged and counted, the applied index stays, and the failure propagates so that the entry
/// stays pending and later writes are refused.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaGroupApplier
{
    /// <summary>The largest number of committed entries read from the log in one batch.</summary>
    private const int BatchSize = 1024;

    private readonly string _nodeId;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly ILogger _log;
    private readonly ReplicationMetrics? _metrics;
    private ulong _appliedIndex;

    /// <summary>The message of the inconsistent record last reported; it names the log index and the reason.</summary>
    private string? _lastReported;

    /// <summary>Whether the applied index was seeded from the durable one; read and written only by the serialized caller of the catch-up.</summary>
    private bool _seeded;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupApplier" /> class.</summary>
    /// <param name="local">Local cache pipeline the entries are applied to.</param>
    /// <param name="log">Logger of the inconsistent records.</param>
    /// <param name="groupId">Identifier of the replica group, the group label of the metric and the diagnostics.</param>
    /// <param name="nodeId">Identifier of this node, the node label of the metric.</param>
    /// <param name="metrics">Replication metrics counting the inconsistent records; nothing is counted when not set.</param>
    internal ReplicaGroupApplier(ILogicalNamespacedCache<object?> local, ILogger log, string groupId = "", string nodeId = "", ReplicationMetrics? metrics = null)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(groupId);
        ArgumentNullException.ThrowIfNull(nodeId);
        _local = local;
        GroupId = groupId;
        _nodeId = nodeId;
        _log = log;
        _metrics = metrics;
    }

    /// <summary>Gets the highest log index applied to memory: the seeded durable applied index, then the last entry applied.</summary>
    /// <remarks>Every entry at or below it has returned from its apply, so its cache journal frame is appended.</remarks>
    internal ulong AppliedIndex => Volatile.Read(ref _appliedIndex);

    /// <summary>Gets the identifier of the replica group whose entries this applier applies.</summary>
    internal string GroupId { get; }

    /// <summary>Applies the next committed entry to memory and advances the applied index to it.</summary>
    /// <param name="logIndex">The log index of the entry.</param>
    /// <param name="canonicalPayload">The canonical record bytes of the entry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The entry is not the one after the applied index.</exception>
    /// <exception cref="InvalidDataException">The payload does not decode or the record is inconsistent.</exception>
    /// <remarks>The order and consistency checks run before anything is applied, so an entry out of order or inconsistent never reaches memory.</remarks>
    internal ValueTask ApplyAsync(ulong logIndex, ReadOnlyMemory<byte> canonicalPayload, CancellationToken cancellationToken) =>
        ApplyCoreAsync(logIndex, canonicalPayload, null, cancellationToken);

    /// <summary>Rebuilds the uncommitted leader tail, logging and counting an inconsistent record before it refuses the tail.</summary>
    /// <param name="tail">The leader tail read from the owned log.</param>
    /// <param name="term">The leader's current term.</param>
    /// <param name="rebuilder">Rebuilds the prepared form of each entry from its record.</param>
    /// <returns>The recovered tail, or <see langword="null" /> when the log tail is fully committed.</returns>
    /// <exception cref="InvalidDataException">A tail record is undecodable or inconsistent, so the committer must not start.</exception>
    /// <remarks>Leader-only: a follower group has no coordinator and recovers no tail.</remarks>
    internal ReplicaRecoveredTail? RecoverTail(ReplicaLeaderTail tail, ulong term, IReplicaTailRebuilder rebuilder)
    {
        ArgumentNullException.ThrowIfNull(tail);
        try
        {
            return tail.IsEmpty ? null : new ReplicaRecoveredTail(tail.Entries, term, rebuilder);
        }
        catch (InvalidDataException error)
        {
            ReportInconsistentRecord(error);
            throw;
        }
    }

    /// <summary>Applies the committed entries memory may lack, so it holds every entry through <paramref name="commitIndex" />.</summary>
    /// <param name="log">The group log.</param>
    /// <param name="durableAppliedIndex">The durable applied index of <paramref name="log" />, which seeds the applied index on the first call and raises it on a later call when it is higher.</param>
    /// <param name="commitIndex">The durable commit index of <paramref name="log" />.</param>
    /// <param name="cancellationToken">Cancellation token for reading the log; the applies themselves are not canceled.</param>
    /// <returns>A task that completes when the applied index reaches <paramref name="commitIndex" />.</returns>
    /// <exception cref="InvalidOperationException">The retained committed entries do not reach <paramref name="commitIndex" /> densely.</exception>
    /// <exception cref="InvalidDataException">A committed entry is inconsistent; it and every later one stay unapplied.</exception>
    /// <remarks>
    /// Callers serialize the calls and the applies of one applier. After a restart memory holds at most what the cache journal kept,
    /// which may miss the entries above the durable applied index: they are applied again, in log order, in batches read from the log.
    /// The applied index is seeded only once, so a resync in this process keeps the in-memory index and applies nothing twice. A later
    /// durable applied index above the in-memory one means the log is applied beyond what memory received (for example after a
    /// snapshot install): the index is raised to it and the gap is logged, as memory lacks those entries. The entries belong to no
    /// caller of the current execution, which can carry the idempotency scope of the write that started the committer; that scope would
    /// stamp their frames with a foreign operation id and count them as that RPC's effect, so they are applied on a pool thread started
    /// without the execution context. With no scope to inherit, the apply still marks itself as replicated, so each entry skips the wait
    /// for its own node journal flush instead of holding the committer gate for one flush per entry. Once the outcomes of the group log are
    /// rebuilt, the outcome of each applied entry is recorded in its idempotency state, so a retry that reaches this node with the group
    /// finds it; before the rebuild nothing is recorded, as the rebuild restores the outcome of every committed entry from the log.
    /// </remarks>
    internal async Task CatchUpAsync(IFollowerLog log, ulong durableAppliedIndex, ulong commitIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        if (!_seeded)
        {
            Volatile.Write(ref _appliedIndex, durableAppliedIndex);
            _seeded = true;
        }
        else if (durableAppliedIndex > AppliedIndex)
        {
            var from = AppliedIndex;
            Volatile.Write(ref _appliedIndex, durableAppliedIndex);
            ServerLog.ReplicaAppliedIndexReseeded(_log, GroupId, from, durableAppliedIndex);
        }

        if (AppliedIndex >= commitIndex)
            return;

        // The flow suppression covers only the start, and must be undone on this thread before the apply is awaited.
        Task reapply;
        if (ExecutionContext.IsFlowSuppressed())
        {
            reapply = StartReapplyAsync(log, commitIndex, cancellationToken);
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
                reapply = StartReapplyAsync(log, commitIndex, cancellationToken);
        }

        await reapply.ConfigureAwait(false);
    }

    /// <summary>Persists the in-memory applied index of the group log once the cache journal holds every applied entry durably.</summary>
    /// <param name="log">The group log.</param>
    /// <param name="durability">The node cache journal whose frames the applies appended.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the durable applied index is at least the in-memory one read at the start.</returns>
    /// <exception cref="InvalidOperationException">The group log refused the applied advance.</exception>
    /// <remarks>
    /// Runs outside the commit gate. Every entry at or below the applied index read here returned from its apply, which appends its
    /// cache journal frame first, so the durability barrier awaited next covers all of them; only then does the log advance its applied
    /// index and release the applied payloads, so a crash never leaves the log claiming an apply the cache journal lost. Nothing is
    /// done while the durable applied index is already there.
    /// </remarks>
    internal async Task FlushAsync(IFollowerLog log, IJournalDurabilityCoordinator durability, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(durability);
        var applied = AppliedIndex;
        var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
        if (applied <= status.LastAppliedIndex)
            return;

        await durability.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
        var result = await log.AdvanceAppliedAsync(applied, cancellationToken).ConfigureAwait(false);
        if (!result.Success)
            throw new InvalidOperationException($"Local group applied advance was refused: {result.RefusalCode}.");
    }

    /// <summary>Builds the outcome a committed record carries, refusing a record of another position.</summary>
    /// <param name="logIndex">The log index of the entry.</param>
    /// <param name="record">The decoded record of the entry.</param>
    /// <returns>The resolved outcome.</returns>
    /// <exception cref="InvalidDataException">The record names another log index or carries no valid decision time.</exception>
    private static GroupIdempotencyRecord BuildOutcome(ulong logIndex, in ReplicaLogRecord record) => record.LogIndex == logIndex
        ? ReplicaOutcomeRecovery.OutcomeOf(in record)
        : ThrowHelper.Throw<GroupIdempotencyRecord>(new InvalidDataException($"Replica log entry {logIndex} carries the record of entry {record.LogIndex}."));

    /// <summary>Applies the next committed entry to memory, records its outcome when asked to, and advances the applied index to it.</summary>
    /// <param name="logIndex">The log index of the entry.</param>
    /// <param name="canonicalPayload">The canonical record bytes of the entry.</param>
    /// <param name="outcomes">The group idempotency state the outcome of the entry is recorded in, or <see langword="null" /> to record none.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The entry is not the one after the applied index.</exception>
    /// <exception cref="InvalidDataException">The payload does not decode, the record is inconsistent, or its outcome cannot be read.</exception>
    /// <remarks>
    /// The outcome is built with the other checks, so a record whose outcome cannot be read never reaches memory, and it is recorded
    /// before the applied index moves past the entry: a retry that finds the entry applied also finds its outcome. The caller has checked
    /// that the outcomes of the group log are rebuilt, which never reverts, so recording the outcome cannot fail after the effect ran.
    /// </remarks>
    private async ValueTask ApplyCoreAsync(ulong logIndex, ReadOnlyMemory<byte> canonicalPayload, GroupIdempotencyState? outcomes, CancellationToken cancellationToken)
    {
        var next = AppliedIndex + 1;
        if (logIndex != next)
            throw new InvalidOperationException($"Replica log entry {logIndex} cannot be applied: the next entry to apply is {next}.");

        ReplicaLogRecord record;
        ReplicaEffectKind effect;
        NodeCacheEntry<object?>? entry;
        GroupIdempotencyRecord outcome = default;
        try
        {
            record = ReplicaLogCodec.Decode(canonicalPayload) ??
                ThrowHelper.Throw<ReplicaLogRecord>(new InvalidDataException($"Replica log entry {logIndex} carries an undecodable canonical payload."));
            effect = ReplicaCacheApplier.Resolve(in record, out entry);
            if (outcomes != null)
                outcome = BuildOutcome(logIndex, in record);
        }
        catch (InvalidDataException error)
        {
            ReportInconsistentRecord(error);
            throw;
        }

        // The group log is the durable source of a replicated write: its cache journal frame must not become an RPC write-ahead
        // intent, or a restart would rebuild a Started record that hides the group outcome the committer replays. The suspension holds
        // with or without an RPC scope, and it also makes the apply skip the wait for its own flush: the entry is already durable in the
        // group log, and the flush of the applied index waits for the node journal before it advances the durable index. A leader-term
        // no-op names no cache: nothing runs for it, and only the applied index moves past it.
        if (effect != ReplicaEffectKind.NoCacheEffect)
        {
            using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
            {
                // The entry is committed: the operation took effect even when its effect writes no cache frame.
                RpcMutationIdempotencyExecutionAmbient.NotifyMutationApplied();
                await ReplicaCacheApplier.ExecuteAsync(_local, record, effect, entry, cancellationToken).ConfigureAwait(false);
            }
        }

        outcomes?.RecordCommittedOutcome(in outcome);
        Volatile.Write(ref _appliedIndex, logIndex);
    }

    private async Task ReapplyCoreAsync(IFollowerLog log, ulong commitIndex, CancellationToken cancellationToken)
    {
        // Read once before any effect: the rebuild never reverts, so once the effect of an entry ran its outcome stays recordable. Before
        // the rebuild every entry applied here is still on the log, and the rebuild restores its outcome. The driver of this applier never
        // runs the rebuild and the catch-up of the group at the same time.
        var outcomes = log.Idempotency.OutcomesRebuilt ? log.Idempotency : null;

        while (AppliedIndex < commitIndex)
        {
            var batch = await log.GetCommittedEntriesAsync(AppliedIndex, BatchSize, cancellationToken).ConfigureAwait(false);
            if (batch.Count == 0)
                throw new InvalidOperationException($"The group log retains no entry {AppliedIndex + 1} to apply through its commit index {commitIndex}.");

            foreach (var entry in batch)
            {
                if (entry.LogIndex > commitIndex)
                    break;

                await ApplyCoreAsync(entry.LogIndex, entry.Payload, outcomes, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Logs and counts a log record that was refused as inconsistent.</summary>
    /// <param name="error">Why the record was refused; it names the log index.</param>
    /// <remarks>
    /// The same record is refused on every attempt (each write, each readiness pass, each restart step), so it is reported only when it
    /// differs from the last one reported: the count and the log follow records, not attempts.
    /// </remarks>
    private void ReportInconsistentRecord(InvalidDataException error)
    {
        if (string.Equals(Interlocked.Exchange(ref _lastReported, error.Message), error.Message, StringComparison.Ordinal))
            return;

        ServerLog.ReplicaInconsistentRecord(_log, GroupId, error);
        _metrics?.ReportInconsistentRecord(_nodeId, GroupId);
    }

    private Task StartReapplyAsync(IFollowerLog log, ulong commitIndex, CancellationToken cancellationToken) => Task.Factory.StartNew(
            () => ReapplyCoreAsync(log, commitIndex, cancellationToken),
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default)
        .Unwrap();
}
