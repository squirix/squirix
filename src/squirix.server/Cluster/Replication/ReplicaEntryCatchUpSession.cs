using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Brings one follower up to the leader's last log index by sending retained leader entries through a catch-up lease.</summary>
/// <remarks>
/// The target is the leader's last index at the start, read after the sender was paused: every entry appended later waits on the
/// sender and is sent by the resumed loop in order. Each round reads one batch from the leader log under its gate and sends it as one
/// append; a log mismatch backs the next index up from the index the follower reported, and a success advances it. The session ends
/// with the empty probe at the target, which proves by Log Matching that the follower holds the leader log through it. It sends
/// nothing it cannot verify: a predecessor below the leader's retained range ends the session as
/// <see cref="ReplicaCatchUpOutcome.Compacted" />. The session reports; it touches neither eligibility nor the quorum.
/// </remarks>
[Immutable]
[SuppressMessage("Usage", "MA0182:Internal type is apparently never used", Justification = "Activation seam until the readiness service drives catch-up sessions in the next change.")]
internal sealed class ReplicaEntryCatchUpSession
{
    private readonly IFollowerLog _leaderLog;
    private readonly string _leaderNodeId;
    private readonly ulong _leaderTerm;
    private readonly int _maxBackUps;
    private readonly long _maxBatchBytes;
    private readonly int _maxBatchEntries;

    /// <summary>Initializes a new instance of the <see cref="ReplicaEntryCatchUpSession" /> class.</summary>
    /// <param name="leaderLog">The leader log the entries are read from.</param>
    /// <param name="leaderNodeId">The leader node identity carried by every request.</param>
    /// <param name="leaderTerm">The leader term carried by every request.</param>
    /// <param name="maxBatchEntries">The most entries one request carries.</param>
    /// <param name="maxBatchBytes">The most canonical payload bytes one request carries; a single larger entry still goes out alone.</param>
    /// <param name="maxBackUps">The most log mismatches the session follows before it gives the follower up as diverged.</param>
    internal ReplicaEntryCatchUpSession(
        IFollowerLog leaderLog,
        string leaderNodeId,
        ulong leaderTerm,
        int maxBatchEntries = 64,
        long maxBatchBytes = 4 * 1024 * 1024,
        int maxBackUps = 64)
    {
        ArgumentNullException.ThrowIfNull(leaderLog);
        ArgumentException.ThrowIfNullOrWhiteSpace(leaderNodeId);
        ArgumentOutOfRangeException.ThrowIfZero(leaderTerm);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBatchEntries, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBatchBytes, 1L);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBackUps, 1);

        _leaderLog = leaderLog;
        _leaderNodeId = leaderNodeId;
        _leaderTerm = leaderTerm;
        _maxBatchEntries = maxBatchEntries;
        _maxBatchBytes = maxBatchBytes;
        _maxBackUps = maxBackUps;
    }

    /// <summary>Runs the session over <paramref name="lease" /> until the follower holds the leader's last index or a round ends it.</summary>
    /// <param name="lease">The catch-up lease of the follower's sender; the session sends only through it.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the session by throwing.</param>
    /// <returns>The outcome and what the follower is verified to hold.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<ReplicaCatchUpResult> RunAsync(ReplicaFollowerCatchUp lease, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(lease);

        var target = (await _leaderLog.GetStatusAsync(cancellationToken).ConfigureAwait(false)).LastLogIndex;
        var state = new Progress(target + 1UL);
        while (true)
        {
            if (await RoundAsync(lease, state, target, cancellationToken).ConfigureAwait(false) is { } outcome)
                return state.End(outcome);
        }
    }

    /// <summary>Maps a failed send to the outcome that ends the session, or rethrows a cancellation the caller requested.</summary>
    /// <param name="lease">The lease the request went through.</param>
    /// <param name="exception">The failure.</param>
    /// <param name="cancellationToken">The session token.</param>
    /// <returns>The outcome, or <see langword="null" /> when the failure is not one of the session's.</returns>
    private static ReplicaCatchUpOutcome? Classify(ReplicaFollowerCatchUp lease, Exception exception, CancellationToken cancellationToken) =>
        exception switch
        {
            OperationCanceledException when cancellationToken.IsCancellationRequested => null,
            OperationCanceledException => lease.IsClosed ? ReplicaCatchUpOutcome.Aborted : ReplicaCatchUpOutcome.Unreachable,
            ObjectDisposedException => ReplicaCatchUpOutcome.Aborted,
            RpcException or IOException or HttpRequestException or TimeoutException or InvalidOperationException => ReplicaCatchUpOutcome.Unreachable,
            _ => null,
        };

    /// <summary>Applies the follower's answer: advances on a success, backs up on a log mismatch, ends on any other refusal.</summary>
    /// <param name="lease">The lease, told what the follower now holds.</param>
    /// <param name="state">The session position.</param>
    /// <param name="read">The leader read the request was built from.</param>
    /// <param name="records">The records the request carried.</param>
    /// <param name="result">The follower's answer.</param>
    /// <returns>The outcome that ends the session, or <see langword="null" /> to run another round.</returns>
    private ReplicaCatchUpOutcome? Apply(ReplicaFollowerCatchUp lease, Progress state, in FollowerLogEntriesRead read, ReplicaLogRecord[] records, in FollowerLogAppendResult result)
    {
        if (result.Success)
        {
            if (records.Length == 0 && result.LastLogIndex != read.PrevLogIndex)
                return ReplicaCatchUpOutcome.Diverged;

            if (records.Length == 0)
                state.Advance(read.PrevLogIndex, read.PrevLogTerm, 0);
            else
                state.Advance(records[^1].LogIndex, records[^1].Term, records.Length);

            lease.MarkHeld(state.HeldThrough);
            return records.Length == 0 ? ReplicaCatchUpOutcome.CaughtUp : null;
        }

        switch (result.RefusalCode)
        {
            case RefusalCodes.LogMismatch:
                var candidate = ReplicaRepairPlanner.BackUpNextIndex(state.Next, result.LastLogIndex);
                if (candidate >= state.Next || ++state.BackUps > _maxBackUps)
                    return ReplicaCatchUpOutcome.Diverged;

                state.Next = candidate;
                return null;
            case RefusalCodes.StaleTerm:
                state.FollowerTerm = result.CurrentTerm;
                return ReplicaCatchUpOutcome.StaleTerm;
            default:
                return ReplicaCatchUpOutcome.Refused;
        }
    }

    /// <summary>Runs one round: one gated read of the leader log and one append through the lease.</summary>
    /// <param name="lease">The lease.</param>
    /// <param name="state">The session position.</param>
    /// <param name="target">The last index the session sends.</param>
    /// <param name="cancellationToken">The session token.</param>
    /// <returns>The outcome that ends the session, or <see langword="null" /> to run another round.</returns>
    private async Task<ReplicaCatchUpOutcome?> RoundAsync(ReplicaFollowerCatchUp lease, Progress state, ulong target, CancellationToken cancellationToken)
    {
        FollowerLogEntriesRead read;
        try
        {
            // Past the target the read carries nothing: the empty probe at the target ends the session.
            read = await _leaderLog.ReadEntriesAsync(state.Next, state.Next > target ? 0 : _maxBatchEntries, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException)
        {
            return ReplicaCatchUpOutcome.Corrupt;
        }

        if (!read.Retained)
            return read.PrevLogIndex > read.LastLogIndex ? ReplicaCatchUpOutcome.Diverged : ReplicaCatchUpOutcome.Compacted;

        if (!TryTakeBatch(read.Entries, target, out var records))
            return ReplicaCatchUpOutcome.Corrupt;

        var batch = new FollowerBatch(records, _leaderNodeId, _leaderTerm, read.PrevLogIndex, read.PrevLogTerm, read.CommitIndex);
        state.Rounds++;
        FollowerLogAppendResult result;
        try
        {
            result = await lease.SendAsync(in batch, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (Classify(lease, exception, cancellationToken) is { } outcome)
        {
            return outcome;
        }

        return Apply(lease, state, in read, records, in result);
    }

    /// <summary>Decodes the entries of one request: those through the target that fit the byte cap, a single larger entry alone.</summary>
    /// <param name="entries">The entries read, in index order.</param>
    /// <param name="target">The last index the session sends.</param>
    /// <param name="records">The decoded records of the request.</param>
    /// <returns><see langword="false" /> when an entry carries an undecodable payload.</returns>
    private bool TryTakeBatch(IReadOnlyList<FollowerLogEntry> entries, ulong target, out ReplicaLogRecord[] records)
    {
        var count = 0;
        var bytes = 0L;
        while (count < entries.Count && entries[count].LogIndex <= target && (count == 0 || bytes + entries[count].Payload.Length <= _maxBatchBytes))
        {
            bytes += entries[count].Payload.Length;
            count++;
        }

        records = count == 0 ? [] : new ReplicaLogRecord[count];
        for (var i = 0; i < count; i++)
        {
            if (ReplicaLogCodec.Decode(entries[i].Payload) is not { } record)
                return false;

            records[i] = record;
        }

        return true;
    }

    /// <summary>The position of one running session.</summary>
    private sealed class Progress
    {
        internal Progress(ulong next)
        {
            Next = next;
        }

        internal int BackUps { get; set; }

        internal int EntriesSent { get; private set; }

        internal ulong FollowerTerm { get; set; }

        internal ulong HeldTerm { get; private set; }

        internal ulong HeldThrough { get; private set; }

        internal ulong Next { get; set; }

        internal int Rounds { get; set; }

        internal void Advance(ulong heldThrough, ulong heldTerm, int entriesSent)
        {
            HeldThrough = heldThrough;
            HeldTerm = heldTerm;
            Next = heldThrough + 1UL;
            EntriesSent += entriesSent;
        }

        internal ReplicaCatchUpResult End(ReplicaCatchUpOutcome outcome) => new(outcome, HeldThrough, HeldTerm, FollowerTerm, EntriesSent, Rounds);
    }
}
