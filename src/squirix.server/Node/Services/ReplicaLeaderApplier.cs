using System;
using System.Collections.Generic;
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
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Applies the committed entries of the owned replica group to local memory, densely in log order, and tracks how far they are applied.</summary>
/// <remarks>
/// The committer owns one instance for its whole lifetime, so the applied index survives resyncs and coordinator replacement. Every
/// apply path goes through <see cref="ApplyAsync" />: the commits of the running coordinator (own and late-majority entries alike) and
/// the re-apply of committed entries at start. The callers serialize the applies (the coordinator's commit gate, or the committer
/// gate while no coordinator runs); the applied index is published with volatile writes because the applied-index flush reads it
/// outside both gates. An apply executes the effect its record carries and reads no clock. A record that is inconsistent, or cannot
/// be decoded, is never applied: it is logged and counted, the applied index stays, and the failure propagates so that the entry
/// stays pending and later writes are refused.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaLeaderApplier
{
    private readonly string _groupId;
    private readonly string _nodeId;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly ILogger _log;
    private readonly ReplicationMetrics? _metrics;
    private ulong _appliedIndex;

    /// <summary>The message of the inconsistent record last reported; it names the log index and the reason.</summary>
    private string? _lastReported;

    /// <summary>Whether the applied index was seeded from the durable one; read and written under the committer gate only.</summary>
    private bool _seeded;

    /// <summary>Initializes a new instance of the <see cref="ReplicaLeaderApplier" /> class.</summary>
    /// <param name="local">Local cache pipeline the entries are applied to.</param>
    /// <param name="log">Logger of the inconsistent records.</param>
    /// <param name="groupId">Identifier of the owned replica group, for diagnostics.</param>
    /// <param name="nodeId">Identifier of this node, the node label of the metric.</param>
    /// <param name="metrics">Replication metrics counting the inconsistent records; nothing is counted when not set.</param>
    internal ReplicaLeaderApplier(ILogicalNamespacedCache<object?> local, ILogger log, string groupId = "", string nodeId = "", ReplicationMetrics? metrics = null)
    {
        ArgumentNullException.ThrowIfNull(local);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(groupId);
        ArgumentNullException.ThrowIfNull(nodeId);
        _local = local;
        _groupId = groupId;
        _nodeId = nodeId;
        _log = log;
        _metrics = metrics;
    }

    /// <summary>Gets the highest log index applied to memory: the seeded durable applied index, then the last entry applied.</summary>
    /// <remarks>Every entry at or below it has returned from its apply, so its cache journal frame is appended.</remarks>
    internal ulong AppliedIndex => Volatile.Read(ref _appliedIndex);

    /// <summary>Applies the next committed entry to memory and advances the applied index to it.</summary>
    /// <param name="logIndex">The log index of the entry.</param>
    /// <param name="canonicalPayload">The canonical record bytes of the entry.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The entry is not the one after the applied index.</exception>
    /// <exception cref="InvalidDataException">The payload does not decode or the record is inconsistent.</exception>
    /// <remarks>The order and consistency checks run before anything is applied, so an entry out of order or inconsistent never reaches memory.</remarks>
    internal async ValueTask ApplyAsync(ulong logIndex, ReadOnlyMemory<byte> canonicalPayload, CancellationToken cancellationToken)
    {
        var next = AppliedIndex + 1;
        if (logIndex != next)
            throw new InvalidOperationException($"Replica log entry {logIndex} cannot be applied: the next entry to apply is {next}.");

        ReplicaLogRecord record;
        ReplicaEffectKind effect;
        NodeCacheEntry<object?>? entry;
        try
        {
            record = ReplicaLogCodec.Decode(canonicalPayload) ??
                ThrowHelper.Throw<ReplicaLogRecord>(new InvalidDataException($"Replica log entry {logIndex} carries an undecodable canonical payload."));
            effect = ReplicaCacheApplier.Resolve(in record, out entry);
        }
        catch (InvalidDataException error)
        {
            ReportInconsistentRecord(error);
            throw;
        }

        // The group log is the durable source of a replicated write: its cache journal frame must not become an RPC write-ahead
        // intent, or a restart would rebuild a Started record that hides the group outcome the committer replays.
        using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
            await ReplicaCacheApplier.ExecuteAsync(_local, record, effect, entry, cancellationToken).ConfigureAwait(false);

        Volatile.Write(ref _appliedIndex, logIndex);
    }

    /// <summary>Rebuilds the uncommitted leader tail, logging and counting an inconsistent record before it refuses the tail.</summary>
    /// <param name="tail">The leader tail read from the owned log.</param>
    /// <param name="term">The leader's current term.</param>
    /// <param name="rebuilder">Rebuilds the prepared form of each entry from its record.</param>
    /// <returns>The recovered tail, or <see langword="null" /> when the log tail is fully committed.</returns>
    /// <exception cref="InvalidDataException">A tail record is undecodable or inconsistent, so the committer must not start.</exception>
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

    /// <summary>Logs and counts a log record that was refused as inconsistent.</summary>
    /// <param name="error">Why the record was refused; it names the log index.</param>
    /// <remarks>
    /// The same record is refused on every attempt (each write, each readiness pass, each restart step), so it is reported only when it
    /// differs from the last one reported: the count and the log follow records, not attempts.
    /// </remarks>
    internal void ReportInconsistentRecord(InvalidDataException error)
    {
        if (string.Equals(Interlocked.Exchange(ref _lastReported, error.Message), error.Message, StringComparison.Ordinal))
            return;

        ServerLog.ReplicaInconsistentRecord(_log, _groupId, error);
        _metrics?.ReportInconsistentRecord(_nodeId, _groupId);
    }

    /// <summary>Applies the committed entries memory may lack, so it holds every entry through <paramref name="commitIndex" />.</summary>
    /// <param name="log">The owned group log.</param>
    /// <param name="durableAppliedIndex">The durable applied index of <paramref name="log" />, which seeds the applied index on the first call.</param>
    /// <param name="commitIndex">The durable commit index of <paramref name="log" />.</param>
    /// <param name="cancellationToken">Cancellation token for reading the log; the applies themselves are not canceled.</param>
    /// <returns>A task that completes when the applied index reaches <paramref name="commitIndex" />.</returns>
    /// <exception cref="InvalidOperationException">The retained committed entries do not reach <paramref name="commitIndex" /> densely.</exception>
    /// <exception cref="InvalidDataException">A committed entry is inconsistent; it and every later one stay unapplied.</exception>
    /// <remarks>
    /// Called under the committer gate, while no coordinator runs. After a restart memory holds at most what the cache journal kept,
    /// which may miss the entries above the durable applied index: they are applied again, in log order. The applied index is seeded
    /// only once, so a resync in this process keeps the in-memory index and applies nothing twice. The entries belong to no caller of
    /// the current execution, which can carry the idempotency scope of the write that started the committer; that scope would defer their
    /// durability to a foreign RPC's outcome and count them as that RPC's effect, so they are applied on a pool thread started without the
    /// execution context.
    /// </remarks>
    internal async Task CatchUpAsync(IFollowerLog log, ulong durableAppliedIndex, ulong commitIndex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(log);
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
            reapply = StartReapplyAsync(committed, commitIndex);
        }
        else
        {
            using (ExecutionContext.SuppressFlow())
                reapply = StartReapplyAsync(committed, commitIndex);
        }

        await reapply.ConfigureAwait(false);
    }

    private async Task ReapplyCoreAsync(IReadOnlyList<FollowerLogEntry> committed, ulong commitIndex)
    {
        foreach (var entry in committed)
        {
            if (entry.LogIndex > commitIndex)
                break;

            if (entry.LogIndex > AppliedIndex)
                await ApplyAsync(entry.LogIndex, entry.Payload, CancellationToken.None).ConfigureAwait(false);
        }

        if (AppliedIndex < commitIndex)
            throw new InvalidOperationException($"The owned group log retains no entry {AppliedIndex + 1} to apply through its commit index {commitIndex}.");
    }

    private Task StartReapplyAsync(IReadOnlyList<FollowerLogEntry> committed, ulong commitIndex) => Task.Factory.StartNew(
            () => ReapplyCoreAsync(committed, commitIndex),
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default)
        .Unwrap();
}
