using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Receives a state transfer from a group leader into the copy of this node, for every group the node serves.</summary>
/// <remarks>
/// <para>
/// A group holds at most one transfer, guarded by an install gate with a ticket. The begin writes nothing: it checks the copy, waits for local
/// journal recovery, takes the apply driver of the group through the <see cref="IReplicaStateSink" />, and creates the ticket. The first wipe
/// step is the first durable write, and it marks the copy incomplete in both metadata slots before any key is removed; the end writes the
/// complete record last, after the entries are written and the journal durability barrier is awaited.
/// </para>
/// <para>
/// Every message carries the transfer id and a sequence number, the begin being zero; another id or sequence number is refused as stale and changes
/// nothing, and a ticket whose term fell below the term of the copy ends. A higher-term begin takes the group over at once, and a ticket silent
/// for the idle timeout is dropped lazily at the next begin and by a timer. Every accepted message is reported to the election state of the
/// group as a contact of the leader, so the member does not campaign against a leader that is working through a long transfer. A transfer that
/// ends for any reason other than completion leaves the copy as it was, complete or incomplete, and releases the group.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaTransferReceiver : IDisposable
{
    private readonly TimeProvider _clock;
    private readonly ILogger<ReplicaTransferReceiver> _log;
    private readonly IReplicaMembership _members;
    private readonly ReplicaTransferOptions _options;
    private readonly ReplicaGroupRegistry _registry;
    private readonly IReplicaStateSink _sink;
    private readonly FrozenDictionary<string, ReplicaTransferSlot> _slots;

    /// <summary>Initializes a new instance of the <see cref="ReplicaTransferReceiver" /> class.</summary>
    /// <param name="registry">Replica group registry of this node; each of its groups gets an install gate.</param>
    /// <param name="members">The replica sets of the served groups, which bound who may send a transfer.</param>
    /// <param name="sink">The port to the cache of the node.</param>
    /// <param name="options">The idle timeout, the wipe step and the snapshot limit.</param>
    /// <param name="clock">Time source of the idle timeout and the transfer duration.</param>
    /// <param name="log">Logger of the transfers.</param>
    internal ReplicaTransferReceiver(
        ReplicaGroupRegistry registry,
        IReplicaMembership members,
        IReplicaStateSink sink,
        ReplicaTransferOptions options,
        TimeProvider clock,
        ILogger<ReplicaTransferReceiver> log)
    {
        ArgumentNullException.ThrowIfNull(registry);
        ArgumentNullException.ThrowIfNull(members);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(log);
        _registry = registry;
        _members = members;
        _sink = sink;
        _options = options;
        _clock = clock;
        _log = log;
        var ended = new Action<ReplicaTransferTicket, ReplicaTransferEnd>(OnEnded);
        var slots = new Dictionary<string, ReplicaTransferSlot>(registry.GroupIds.Count, StringComparer.Ordinal);
        for (var i = 0; i < registry.GroupIds.Count; i++)
            slots.Add(registry.GroupIds[i], new ReplicaTransferSlot(clock, options.IdleTimeout, ended));

        _slots = slots.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var slot in _slots.Values)
            slot.Dispose();
    }

    /// <summary>Aborts the transfer the group holds; the copy stays as it is.</summary>
    /// <param name="header">The transfer to abort, with its verified sender and term.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// Accepted when the transfer was dropped or the group holds none; refused as stale when the group holds another transfer, or the same
    /// transfer of another sender or term.
    /// </returns>
    internal async Task<ReplicaTransferReply> AbortAsync(ReplicaTransferHeader header, CancellationToken cancellationToken)
    {
        var groupId = header.GroupId;
        if (!TryResolve(groupId, out var slot, out var log))
            return Refuse(groupId, 0UL, RefusalCodes.NotMember);

        await slot.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var term = (await log.GetStatusAsync(cancellationToken).ConfigureAwait(false)).CurrentTerm;
            if (slot.Current is not { } held)
                return ReplicaTransferReply.Accepted(term);

            if (held.Header != header)
                return Refuse(groupId, term, RefusalCodes.TransferStale);

            _ = slot.Drop(held, ReplicaTransferEnd.Aborted);
            return ReplicaTransferReply.Accepted(term);
        }
        finally
        {
            _ = slot.Gate.Release();
        }
    }

    /// <summary>Begins a transfer: checks the copy, waits for local journal recovery, takes the apply driver of the group and creates the ticket.</summary>
    /// <param name="header">Who sends the transfer.</param>
    /// <param name="baseline">The point the transfer is taken at and the snapshot image that goes with it.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// Accepted once the group is held; otherwise the refusal: not-member, topology-mismatch, stale-term (also when another leader is known for the term),
    /// checksum-mismatch (also for a declared index term of zero or above the sender's term), unseeded, not-ready (also while this node leads the group,
    /// the log is not ready or the index is below the durable commit index) or transfer-busy. Nothing durable is written.
    /// </returns>
    internal async Task<ReplicaTransferReply> BeginAsync(ReplicaTransferHeader header, ReplicaTransferBaseline baseline, CancellationToken cancellationToken)
    {
        if (!TryResolve(header.GroupId, out var slot, out var log) || !_members.IsMember(header.GroupId, header.LeaderNodeId))
            return Refuse(header.GroupId, 0UL, RefusalCodes.NotMember);

        if (!_options.Admits(in header, in baseline))
            return Refuse(header.GroupId, 0UL, RefusalCodes.ChecksumMismatch);

        await slot.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            var view = _registry.TryGetState(header.GroupId, out var election) ? election.ReadRoute() : default;
            var refusal = ReplicaTransferAdmission.Refusal(in header, in baseline, in status, in view) ?? ReplaceHeld(slot, in header);
            if (refusal == null && !await _sink.EnterAsync(header.GroupId, cancellationToken).ConfigureAwait(false))
                refusal = RefusalCodes.NotReady;

            if (refusal != null)
                return Refuse(header.GroupId, status.CurrentTerm, refusal);

            _registry.TransferHoldFor(header.GroupId).Set(true);
            slot.Open(new ReplicaTransferTicket(in header, in baseline, _clock.GetTimestamp()));
            ReplicaTransferLog.Began(_log, header.GroupId, header.LeaderNodeId, baseline.Index, header.LeaderTerm);
            ObserveContact(header.GroupId, header.LeaderNodeId, status.CurrentTerm);
            return ReplicaTransferReply.Accepted(status.CurrentTerm);
        }
        finally
        {
            _ = slot.Gate.Release();
        }
    }

    /// <summary>Writes a batch of entries through the sink.</summary>
    /// <param name="header">The transfer the batch belongs to, with its verified sender and term.</param>
    /// <param name="sequence">The sequence number of the message.</param>
    /// <param name="entries">The entries, in the order the leader sends them.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Accepted, or the refusal; a batch that does not decode ends the transfer as checksum-mismatch.</returns>
    internal Task<ReplicaTransferReply> EntriesAsync(in ReplicaTransferHeader header, int sequence, ReadOnlyMemory<ReplicaStateEntry> entries, CancellationToken cancellationToken) =>
        StepAsync(
            header,
            new ReplicaTransferMessage { Step = ReplicaTransferStep.Entries, GroupId = header.GroupId, TransferId = header.TransferId, Sequence = sequence, Entries = entries },
            cancellationToken);

    /// <summary>Ends the transfer: compares the count and the checksum of the entries, awaits the journal barrier and completes the copy at the transferred index.</summary>
    /// <param name="header">The transfer to end, with its verified sender and term.</param>
    /// <param name="sequence">The sequence number of the message.</param>
    /// <param name="entryCount">The number of entries the leader sent.</param>
    /// <param name="entriesCrc">The CRC32C the leader computed over the entries.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Accepted once the copy is complete; checksum-mismatch ends the transfer and the copy stays incomplete.</returns>
    internal Task<ReplicaTransferReply> EndAsync(in ReplicaTransferHeader header, int sequence, ulong entryCount, uint entriesCrc, CancellationToken cancellationToken) =>
        StepAsync(
            header,
            new ReplicaTransferMessage
            {
                Step = ReplicaTransferStep.End,
                GroupId = header.GroupId,
                TransferId = header.TransferId,
                Sequence = sequence,
                EntryCount = entryCount,
                EntriesCrc = entriesCrc,
            },
            cancellationToken);

    /// <summary>Ends the transfer the group holds because this node starts leading the group; the copy stays as it is.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the group holds no transfer and its apply driver is free.</returns>
    /// <remarks>
    /// A leadership takes the apply driver of the group, which the transfer holds from its begin. An incomplete copy never campaigns, so a node that
    /// wins an election can only hold a transfer that has not written yet, and its copy is still complete. The message gate is taken, so a message being
    /// handled finishes first, and a begin that arrives later is refused while the node leads.
    /// </remarks>
    internal async Task ReleaseAsync(string groupId, CancellationToken cancellationToken)
    {
        if (!_slots.TryGetValue(groupId, out var slot))
            return;

        // The gate is taken before the ticket is looked at: a begin that holds the apply driver but has not opened its ticket is not missed.
        await slot.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (slot.Current is { } held)
                _ = slot.Drop(held, ReplicaTransferEnd.Superseded);
        }
        finally
        {
            _ = slot.Gate.Release();
        }
    }

    /// <summary>Buffers a chunk of the published snapshot image; the last chunk decodes and checks it.</summary>
    /// <param name="header">The transfer the chunk belongs to, with its verified sender and term.</param>
    /// <param name="sequence">The sequence number of the message.</param>
    /// <param name="offset">The offset of the chunk in the image.</param>
    /// <param name="chunk">The bytes of the chunk.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Accepted, or the refusal; a malformed image ends the transfer as checksum-mismatch and the copy is untouched.</returns>
    internal Task<ReplicaTransferReply> SnapshotAsync(in ReplicaTransferHeader header, int sequence, long offset, ReadOnlyMemory<byte> chunk, CancellationToken cancellationToken) =>
        StepAsync(
            header,
            new ReplicaTransferMessage
            {
                Step = ReplicaTransferStep.Snapshot,
                GroupId = header.GroupId,
                TransferId = header.TransferId,
                Sequence = sequence,
                Offset = offset,
                Chunk = chunk,
            },
            cancellationToken);

    /// <summary>Removes keys of the group, a bounded number per call; the first call marks the copy incomplete before any key is removed.</summary>
    /// <param name="header">The transfer the step belongs to, with its verified sender and term.</param>
    /// <param name="sequence">The sequence number of the message.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Accepted with <see cref="ReplicaTransferReply.More" /> set until the group has no key left; otherwise the refusal of the copy or of the transfer.</returns>
    internal Task<ReplicaTransferReply> WipeAsync(in ReplicaTransferHeader header, int sequence, CancellationToken cancellationToken) =>
        StepAsync(
            header,
            new ReplicaTransferMessage { Step = ReplicaTransferStep.Wipe, GroupId = header.GroupId, TransferId = header.TransferId, Sequence = sequence },
            cancellationToken);

    /// <summary>Takes over a group whose held transfer is of a lower term or has been silent for the idle timeout.</summary>
    /// <param name="slot">The install gate of the group, whose message gate the caller holds.</param>
    /// <param name="header">The transfer that begins.</param>
    /// <returns><see cref="RefusalCodes.TransferBusy" /> when the held transfer stays; otherwise <see langword="null" />, the group being free.</returns>
    private static string? ReplaceHeld(ReplicaTransferSlot slot, in ReplicaTransferHeader header)
    {
        if (slot.Current is not { } held)
            return null;

        if (header.LeaderTerm > held.Header.LeaderTerm)
            _ = slot.Drop(held, ReplicaTransferEnd.Superseded);
        else if (slot.IsIdle(held))
            _ = slot.Drop(held, ReplicaTransferEnd.Expired);
        else
            return RefusalCodes.TransferBusy;

        return null;
    }

    private async ValueTask<ReplicaTransferReply> DispatchAsync(
        (ReplicaTransferSlot Slot, IFollowerLog Log) group,
        ReplicaTransferTicket ticket,
        ReplicaTransferMessage message,
        ulong term,
        CancellationToken cancellationToken) => message.Step switch
        {
            ReplicaTransferStep.Snapshot => ReceiveChunk(group.Slot, ticket, in message, term),
            ReplicaTransferStep.Wipe => await WipeStepAsync(group, ticket, term, cancellationToken).ConfigureAwait(false),
            ReplicaTransferStep.Entries => await WriteBatchAsync(group.Slot, ticket, message, term, cancellationToken).ConfigureAwait(false),
            ReplicaTransferStep.End => await FinishAsync(group, ticket, message, term, cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentOutOfRangeException(nameof(message), message.Step, "Unsupported transfer step."),
        };

    private ReplicaTransferReply Fail(ReplicaTransferSlot slot, ReplicaTransferTicket ticket, string refusalCode, ulong term)
    {
        _ = slot.Drop(ticket, ReplicaTransferEnd.Failed);
        return Refuse(ticket.Header.GroupId, term, refusalCode);
    }

    private async ValueTask<ReplicaTransferReply> FinishAsync(
        (ReplicaTransferSlot Slot, IFollowerLog Log) group,
        ReplicaTransferTicket ticket,
        ReplicaTransferMessage message,
        ulong term,
        CancellationToken cancellationToken)
    {
        if (ticket.Phase != ReplicaTransferPhase.Entries)
            return Refuse(ticket.Header.GroupId, term, RefusalCodes.TransferStale);

        // The sink awaits the journal durability barrier before it reports what it wrote, so the complete record below follows the barrier.
        var totals = await _sink.SealAsync(ticket.Header.GroupId, cancellationToken).ConfigureAwait(false);
        if (totals.EntryCount != message.EntryCount || totals.Crc != message.EntriesCrc)
            return Fail(group.Slot, ticket, RefusalCodes.ChecksumMismatch, term);

        var installed = await group.Log.CompleteStateInstallAsync(ticket.Snapshot, ticket.Leader, cancellationToken).ConfigureAwait(false);
        if (!installed.Success)
            return Fail(group.Slot, ticket, installed.Refusal, term);

        // The apply driver is still held, so no pass runs between the complete record and the reseed.
        _sink.ResetAppliedTo(ticket.Header.GroupId, ticket.Baseline.Index);
        ticket.Entries = totals.EntryCount;
        _ = group.Slot.Drop(ticket, ReplicaTransferEnd.Completed);
        return ReplicaTransferReply.Accepted(ticket.Header.LeaderTerm);
    }

    private void ObserveContact(string groupId, string leaderNodeId, ulong term)
    {
        if (_registry.TryGetState(groupId, out var election))
            election.ObserveLeaderContact(leaderNodeId, term);
    }

    private void OnEnded(ReplicaTransferTicket ticket, ReplicaTransferEnd end)
    {
        var header = ticket.Header;
        _sink.Leave(header.GroupId);
        _registry.TransferHoldFor(header.GroupId).Set(false);
        _registry.ApplySignalFor(header.GroupId).Notify();
        if (end == ReplicaTransferEnd.Completed)
        {
            var durationMs = long.CreateSaturating(_clock.GetElapsedTime(ticket.StartedAt).TotalMilliseconds);
            ReplicaTransferLog.Completed(_log, header.GroupId, header.LeaderNodeId, ticket.Baseline.Index, ticket.Entries, durationMs);
        }
        else
        {
            ReplicaTransferLog.Dropped(_log, header.GroupId, header.LeaderNodeId, end);
        }
    }

    private ReplicaTransferReply ReceiveChunk(ReplicaTransferSlot slot, ReplicaTransferTicket ticket, in ReplicaTransferMessage message, ulong term)
    {
        if (ticket.Phase != ReplicaTransferPhase.Snapshot)
            return Refuse(ticket.Header.GroupId, term, RefusalCodes.TransferStale);

        if (!ticket.TryReceive(message.Offset, message.Chunk.Span, out var overflow))
            return overflow ? Fail(slot, ticket, RefusalCodes.ChecksumMismatch, term) : Refuse(ticket.Header.GroupId, term, RefusalCodes.TransferStale);

        if (!ticket.IsImageComplete)
            return ReplicaTransferReply.Accepted(term);

        // A malformed image ends the transfer before anything durable was written: the copy is untouched.
        if (!ticket.TryDecode(_options.MaxSnapshotBytes))
            return Fail(slot, ticket, RefusalCodes.ChecksumMismatch, term);

        ticket.ReleaseImage();
        ticket.Phase = ReplicaTransferPhase.Wipe;
        return ReplicaTransferReply.Accepted(term);
    }

    private ReplicaTransferReply Refuse(string groupId, ulong term, string refusalCode)
    {
        ReplicaTransferLog.Refused(_log, groupId, refusalCode);
        return ReplicaTransferReply.Refused(term, refusalCode);
    }

    private async Task<ReplicaTransferReply> StepAsync(ReplicaTransferHeader header, ReplicaTransferMessage message, CancellationToken cancellationToken)
    {
        if (!TryResolve(message.GroupId, out var slot, out var log))
            return Refuse(message.GroupId, 0UL, RefusalCodes.NotMember);

        await slot.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            if (slot.Admit(in header, message.Sequence) is not { } ticket)
                return Refuse(message.GroupId, status.CurrentTerm, RefusalCodes.TransferStale);

            var reply = default(ReplicaTransferReply);
            var handled = false;
            try
            {
                var term = ticket.InstallBegun ? ticket.Header.LeaderTerm : status.CurrentTerm;
                reply = ticket.Header.LeaderTerm < status.CurrentTerm
                    ? Fail(slot, ticket, RefusalCodes.StaleTerm, status.CurrentTerm)
                    : await DispatchAsync((slot, log), ticket, message, term, cancellationToken).ConfigureAwait(false);
                handled = true;
            }
            finally
            {
                // A step that failed leaves the copy as it is and frees the group; the sender begins again.
                if (handled)
                    slot.Finish(ticket, reply.Success);
                else
                    _ = slot.Drop(ticket, ReplicaTransferEnd.Failed);
            }

            if (reply.Success)
                ObserveContact(message.GroupId, ticket.Header.LeaderNodeId, reply.Term);

            return reply;
        }
        finally
        {
            _ = slot.Gate.Release();
        }
    }

    private bool TryResolve(string groupId, [NotNullWhen(true)] out ReplicaTransferSlot? slot, [NotNullWhen(true)] out IFollowerLog? log)
    {
        log = null;
        return _slots.TryGetValue(groupId, out slot) && _registry.TryGetLog(groupId, out log);
    }

    private async ValueTask<ReplicaTransferReply> WipeStepAsync(
        (ReplicaTransferSlot Slot, IFollowerLog Log) group,
        ReplicaTransferTicket ticket,
        ulong term,
        CancellationToken cancellationToken)
    {
        if (ticket.Phase != ReplicaTransferPhase.Wipe)
            return Refuse(ticket.Header.GroupId, term, RefusalCodes.TransferStale);

        // The incomplete record is durable in both metadata slots before the first key is removed; a repeated begin only publishes it again.
        if (!ticket.InstallBegun)
        {
            var baseline = ticket.Baseline;
            var refusal = await group.Log.BeginStateInstallAsync(ticket.Leader, baseline.Index, baseline.Fingerprint, baseline.Generation, ticket.Snapshot, cancellationToken).ConfigureAwait(false);
            if (refusal != null)
                return Fail(group.Slot, ticket, refusal, term);

            ticket.InstallBegun = true;
        }

        var more = await _sink.WipeStepAsync(ticket.Header.GroupId, _options.WipeStepKeys, cancellationToken).ConfigureAwait(false);
        if (!more)
            ticket.Phase = ReplicaTransferPhase.Entries;

        return ReplicaTransferReply.Accepted(ticket.Header.LeaderTerm, more);
    }

    private async ValueTask<ReplicaTransferReply> WriteBatchAsync(
        ReplicaTransferSlot slot,
        ReplicaTransferTicket ticket,
        ReplicaTransferMessage message,
        ulong term,
        CancellationToken cancellationToken)
    {
        if (ticket.Phase != ReplicaTransferPhase.Entries)
            return Refuse(ticket.Header.GroupId, term, RefusalCodes.TransferStale);

        try
        {
            await _sink.WriteAsync(ticket.Header.GroupId, message.Entries, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException)
        {
            return Fail(slot, ticket, RefusalCodes.ChecksumMismatch, term);
        }

        return ReplicaTransferReply.Accepted(term);
    }
}
