using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

/// <summary>Owner-side commit pipeline: local durable append, follower fan-out, and memory apply.</summary>
/// <remarks>
/// All calls originate from the single owning coordinator under its commit gate (ordered commit bodies, and
/// the commit-and-apply of entries a late majority or a later drive covers), except
/// <see cref="RecordLaggingReplica" />, which the coordinator also invokes from background follower
/// observation. The coordinator serializes whole commit bodies under its commit gate, and the committer
/// drives one commit at a time. The local append of a run queues every entry of it on every follower sender right after it succeeds, with nothing
/// in between that could throw, so a locally appended entry always reaches the followers; the fan-out of the coordinator then
/// only takes the queued acknowledgements. Only the commit watermark is shared across the background path and stays monotonic;
/// every other field is written solely by the serialized body. Each follower slot has a <see cref="ReplicaFollowerSender" /> that
/// owns the order of its appends: the sender never sends N+1 before the request that carries N has been answered, so a slow
/// follower is never handed N+1 ahead of N.
/// </remarks>
internal sealed class ReplicaGroupCommitPipeline : IReplicaCommitPipeline
{
    private readonly ReplicaGroupApplier _applier;
    private readonly ReplicaLaggingFollowers _lagging;
    private readonly IFollowerLog _log;
    private readonly string _selfId;
    private readonly ReplicaFollowerSender[] _senders;
    private readonly ReplicaSlots _slots;
    private ulong _commitIndex;
    private Task<ReplicaDurableAcknowledgement>[] _enqueued = [];
    private int _enqueuedCount;
    private ulong _enqueuedFirstIndex;
    private ulong _prevLogIndex;
    private ulong _prevLogTerm;
    private ReplicaLogRecord[] _records = [];

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitPipeline" /> class.</summary>
    /// <param name="applier">The committer's applier, which applies committed entries to memory in log order.</param>
    /// <param name="log">Owned group log for local durable appending.</param>
    /// <param name="senders">The senders of the follower slots, in slot order; the pipeline owns them and closes them.</param>
    /// <param name="leader">This node identifier and its slot in the group, which has no sender.</param>
    /// <param name="lagging">Demotes and queues for repair the followers that did not acknowledge an entry.</param>
    /// <param name="status">Durable log status seeding previous and commit positions.</param>
    /// <param name="term">The leader term the pipeline appends and replicates in.</param>
    internal ReplicaGroupCommitPipeline(
        ReplicaGroupApplier applier,
        IFollowerLog log,
        ReplicaFollowerSender[] senders,
        (string SelfId, int ReplicaIndex) leader,
        ReplicaLaggingFollowers lagging,
        in FollowerLogStatus status,
        ulong term)
    {
        ArgumentNullException.ThrowIfNull(applier);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(senders);
        ArgumentException.ThrowIfNullOrWhiteSpace(leader.SelfId);

        _applier = applier;
        _log = log;
        _selfId = leader.SelfId;
        _slots = new ReplicaSlots(leader.ReplicaIndex);
        _senders = senders;
        _lagging = lagging;
        Term = term;
        _prevLogIndex = status.LastLogIndex;
        _prevLogTerm = status.LastLogTerm;
        _commitIndex = status.CommitIndex;
    }

    /// <summary>Gets the group log index after the last entry this pipeline appended locally, or after the seeded status.</summary>
    /// <remarks>Read by the committer under its gate, between commits, once the commit that last appended has completed.</remarks>
    internal ulong NextLogIndex => _prevLogIndex + 1;

    /// <summary>Gets the leader term the pipeline appends and replicates in.</summary>
    internal ulong Term { get; }

    /// <summary>Initializes the read-index rounds of an elected leader, which observe every follower reply; none for a static leader.</summary>
    /// <remarks>The pipeline closes them together with its senders.</remarks>
    internal ReplicaReadIndexRound? ReadIndex { private get; init; }

    /// <summary>Initializes the record of the local appends, which the committer owns so it outlives this pipeline; a pipeline of its own records for itself.</summary>
    internal ReplicaLocalAppends LocalAppends { private get; init; } = new();

    /// <inheritdoc />
    public async ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        var result = await _log.AdvanceCommitAsync(commitIndex, cancellationToken).ConfigureAwait(false);
        if (!result.Success)
            throw new InvalidOperationException($"Local group commit advance was refused: {result.RefusalCode}.");

        Volatile.Write(ref _commitIndex, result.CommitIndex);
        SendCommitIndex(result.CommitIndex);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Returns the acknowledgement task of the follower's sender for that entry, which the local append of the run already queued it
    /// on; the cancellation token is deliberately ignored, so a late acknowledgement still reaches the observation of the remaining
    /// followers. On the normal path the sender completes the entries of one request in order and the requests one after another, so
    /// the task of the last entry of the run completing means the earlier ones have completed too. That does not hold when the sender
    /// is disposed: it fails the entries still waiting at once and cancels the request in flight afterwards, so the last task can
    /// complete while earlier ones still run. A caller that needs every entry awaits them in log order. The commit itself stops at its
    /// budget while it waits for the majority.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The entry is not part of the run this pipeline appended last, or the replica has no sender.</exception>
    public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
    {
        var inRun = _enqueuedCount != 0 && mutation.LogIndex >= _enqueuedFirstIndex && mutation.LogIndex - _enqueuedFirstIndex < ulong.CreateChecked(_enqueuedCount);
        if (!inRun)
        {
            var message = $"Entry {mutation.LogIndex} was not appended locally by this pipeline; the last run was {_enqueuedFirstIndex}+{_enqueuedCount}.";
            return ValueTask.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException(message));
        }

        if (!_slots.IsFollower(replicaIndex) || replicaIndex < 0 || _slots.SenderOf(replicaIndex) >= _senders.Length)
            return ValueTask.FromException<ReplicaDurableAcknowledgement>(new InvalidOperationException($"Replica {replicaIndex} has no follower sender."));

        var entry = int.CreateChecked(mutation.LogIndex - _enqueuedFirstIndex);
        return new ValueTask<ReplicaDurableAcknowledgement>(_enqueued[(entry * _senders.Length) + _slots.SenderOf(replicaIndex)]);
    }

    /// <inheritdoc />
    /// <remarks>
    /// The entries go to the log as one request. Once it succeeded, every entry is queued on every follower sender in log order, each
    /// naming its predecessor, before anything else runs: queueing never waits and never throws, so an exception later in the commit
    /// cannot leave a locally appended entry unsent. The append is not atomic on disk: a crash can leave a durable prefix of the run,
    /// which recovery keeps as a tail. A rejected run changes nothing: the next index and the last-run lookup stay as they were. If a
    /// sender refuses an entry (backlog full), the rest of the run fails on that sender at once, since that follower never got the
    /// predecessor of the later entries.
    /// </remarks>
    /// <exception cref="ArgumentException"><paramref name="mutations" /> is empty.</exception>
    /// <exception cref="InvalidOperationException">The run is not dense from the next index in one term, or carries an undecodable payload, or the log refused it.</exception>
    public async ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(mutations);
        if (mutations.Count == 0)
            throw new ArgumentException("At least one entry is required.", nameof(mutations));

        var count = mutations.Count;
        var first = mutations[0];
        if (_records.Length < count)
            _records = new ReplicaLogRecord[count];

        try
        {
            var entries = new FollowerLogEntry[count];
            for (var i = 0; i < count; i++)
            {
                var mutation = mutations[i];
                if (mutation.Term != first.Term || mutation.LogIndex != _prevLogIndex + 1 + ulong.CreateChecked(i))
                    throw new InvalidOperationException("Prepared mutations must be a dense run of one term that follows the last appended entry.");
                if (ReplicaLogCodec.Decode(mutation.CanonicalPayload) is not { } record)
                    throw new InvalidOperationException("Prepared mutation carries an undecodable canonical payload.");

                _records[i] = record;
                entries[i] = new FollowerLogEntry(mutation.LogIndex, mutation.Term, mutation.CanonicalPayload);
            }

            var commitIndex = _commitIndex;
            var request = new FollowerLogAppendRequest(_selfId, first.Term, _prevLogIndex, _prevLogTerm, commitIndex, new ReadOnlyMemory<FollowerLogEntry>(entries));
            var result = await AppendToLogAsync(request, cancellationToken).ConfigureAwait(false);
            ThrowIfRefused(in result, first.Term);
            EnqueueRun(mutations, commitIndex);
        }
        finally
        {
            Array.Clear(_records, 0, count);
        }
    }

    /// <inheritdoc />
    /// <remarks>Every entry the coordinator applies, its own and those a late majority commits, advances the committer's applied index.</remarks>
    public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        _applier.ApplyAsync(mutation.LogIndex, mutation.CanonicalPayload, cancellationToken);

    /// <inheritdoc />
    /// <remarks>Called under the commit gate and from background follower observation; it never waits and never throws.</remarks>
    public void RecordLaggingReplica(int replicaIndex, ulong logIndex) => _lagging.Record(replicaIndex, logIndex, _senders[_slots.SenderOf(replicaIndex)].NodeId);

    /// <summary>Sends one heartbeat, carrying the commit index, to every follower whose sender is idle.</summary>
    /// <param name="deferWhileBusy">Whether a busy sender sends the heartbeat once it runs out of entries, as a read-index round needs.</param>
    /// <remarks>Called outside the commit gate; it never waits and never throws, and a closed sender sends nothing.</remarks>
    internal void Heartbeat(bool deferWhileBusy = false)
    {
        var commitIndex = ReadCommitIndex();
        for (var i = 0; i < _senders.Length; i++)
            _ = _senders[i].TryEnqueueHeartbeat(commitIndex, deferWhileBusy);
    }

    /// <summary>Confirms a read index for a leader read: the commit index, once a majority answered this leader in its term after it was taken.</summary>
    /// <param name="cancellationToken">Cancellation token; it ends the wait of this read only.</param>
    /// <returns>The confirmed read index.</returns>
    /// <exception cref="Grpc.Core.RpcException">A follower answered in a higher term (stale-term), or the pipeline closed (Unavailable); nothing was read.</exception>
    /// <exception cref="InvalidOperationException">The pipeline leads statically and confirms no read index.</exception>
    /// <remarks>Called outside the commit gate; readers that arrive while a round is in flight share the next round.</remarks>
    internal ValueTask<ulong> ConfirmReadIndexAsync(CancellationToken cancellationToken) =>
        ReadIndex?.ConfirmAsync(this, static pipeline => pipeline.ReadCommitIndex(), static pipeline => pipeline.Heartbeat(true), cancellationToken) ??
        ValueTask.FromException<ulong>(new InvalidOperationException("A pipeline of a static leader confirms no read index."));

    /// <summary>Gets what a catch-up of a follower slot runs against: this pipeline, the slot's sender, the leader log and term.</summary>
    /// <param name="replicaIndex">Zero-based follower slot, never the leader slot.</param>
    /// <returns>The target.</returns>
    internal ReplicaCatchUpTarget CatchUpTargetFor(int replicaIndex) => new(replicaIndex, this, _senders[_slots.SenderOf(replicaIndex)], _log, Term);

    /// <summary>Stops admitting entries to every follower sender and waits for the queued ones to be answered.</summary>
    /// <param name="budget">The longest wait, shared by all senders.</param>
    /// <returns>A task that completes when every sender is idle or the budget elapsed.</returns>
    internal async ValueTask DrainAsync(TimeSpan budget)
    {
        var draining = new Task[_senders.Length];
        for (var i = 0; i < draining.Length; i++)
            draining[i] = _senders[i].DrainAsync(budget).AsTask();

        await Task.WhenAll(draining).ConfigureAwait(false);
    }

    /// <summary>Closes every follower sender: waiting entries fail and the requests in flight are canceled.</summary>
    /// <returns>The first failure of a sender close, or <see langword="null" />; this call never throws.</returns>
    internal async ValueTask<Exception?> CloseAsync()
    {
        // Reads waiting for a round fail at once: no reply of a closed sender confirms them any more.
        ReadIndex?.Close();
        var closing = new Task<Exception?>[_senders.Length];
        for (var i = 0; i < closing.Length; i++)
            closing[i] = _senders[i].CaptureFailureAsync().AsTask();

        var failures = await Task.WhenAll(closing).ConfigureAwait(false);
        foreach (var failure in failures)
        {
            if (failure != null)
                return failure;
        }

        return null;
    }

    private static Task<ReplicaDurableAcknowledgement> FailedLikeAsync(Task<ReplicaDurableAcknowledgement> refused) =>
        Task.FromException<ReplicaDurableAcknowledgement>(refused.Exception?.InnerException ?? new InvalidOperationException("Follower append was refused."));

    private static ReplicaLocalAppends.Outcome Classify(in FollowerLogAppendResult result) =>
        true switch
        {
            _ when result.Success => ReplicaLocalAppends.Outcome.Appended,
            _ when string.Equals(result.RefusalCode, FollowerLogRefusal.StaleTerm, StringComparison.Ordinal) => ReplicaLocalAppends.Outcome.Ignored,
            _ when string.Equals(result.RefusalCode, FollowerLogRefusal.NotReady, StringComparison.Ordinal) => ReplicaLocalAppends.Outcome.Refused,
            _ => ReplicaLocalAppends.Outcome.Failed,
        };

    /// <summary>Hands a static leader's new commit index to every follower, which would otherwise see it only with the next write.</summary>
    /// <param name="commitIndex">The commit index that was just made durable.</param>
    /// <remarks>
    /// An elected leader heartbeats on its own schedule and sends nothing here. Each sender sends one empty append when idle and defers one
    /// behind the requests of a busy slot, which carry the index themselves; it never waits and never throws.
    /// </remarks>
    private void SendCommitIndex(ulong commitIndex)
    {
        if (ReadIndex != null)
            return;

        for (var i = 0; i < _senders.Length; i++)
            _ = _senders[i].TryEnqueueCommitIndex(commitIndex);
    }

    private void ThrowIfRefused(in FollowerLogAppendResult result, ulong term)
    {
        if (result.Success)
            return;
        if (string.Equals(result.RefusalCode, FollowerLogRefusal.StaleTerm, StringComparison.Ordinal))
            throw new ReplicaTermSupersededException($"Local group append was refused: the log holds a term above the entry term {term}.");
        if (string.Equals(result.RefusalCode, FollowerLogRefusal.NotReady, StringComparison.Ordinal) && _log.IsDisposed)
            throw new ReplicaShutdownRefusedException(nameof(ReplicaGroupCommitPipeline));

        throw new InvalidOperationException($"Local group append was refused: {result.RefusalCode}.");
    }

    /// <summary>Queues every entry of an appended run on every sender in log order and records the run; never waits and never throws.</summary>
    /// <param name="mutations">The run, whose decoded records the pipeline holds.</param>
    /// <param name="commitIndex">The commit index the run carries.</param>
    private void EnqueueRun(IReadOnlyList<PreparedReplicaMutation> mutations, ulong commitIndex)
    {
        // The fan-out of an entry carries that very entry, so it names the predecessor, not the entry itself.
        var count = mutations.Count;
        var enqueued = new Task<ReplicaDurableAcknowledgement>[count * _senders.Length];
        var prevLogIndex = _prevLogIndex;
        var prevLogTerm = _prevLogTerm;
        for (var i = 0; i < count; i++)
        {
            for (var sender = 0; sender < _senders.Length; sender++)
            {
                // A sender that already refused an earlier entry never got the predecessor of this one.
                enqueued[(i * _senders.Length) + sender] = i > 0 && enqueued[((i - 1) * _senders.Length) + sender] is { IsFaulted: true } refused
                    ? FailedLikeAsync(refused)
                    : _senders[sender].EnqueueAsync(mutations[i], in _records[i], prevLogIndex, prevLogTerm, commitIndex, count == 1);
            }

            prevLogIndex = mutations[i].LogIndex;
            prevLogTerm = mutations[i].Term;
        }

        // A run is sent only once all of it is queued, so an idle sender batches it instead of sending its first entry alone.
        if (count > 1)
        {
            for (var sender = 0; sender < _senders.Length; sender++)
                _senders[sender].StartPending();
        }

        _enqueued = enqueued;
        _enqueuedFirstIndex = mutations[0].LogIndex;
        _enqueuedCount = count;
        _prevLogIndex = prevLogIndex;
        _prevLogTerm = prevLogTerm;
    }

    /// <summary>Appends to the owned group log and records how the append ended.</summary>
    /// <param name="request">The append request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The result of the log.</returns>
    /// <remarks>
    /// A refusal for a stale term and an <see cref="ObjectDisposedException" /> are not counted: neither says the log cannot append. A
    /// disposed log that was drained answers not ready instead, which is counted as a refusal. Any other failure, including the budget
    /// running out while the append waits for the gate of the log, is counted.
    /// </remarks>
    private async ValueTask<FollowerLogAppendResult> AppendToLogAsync(FollowerLogAppendRequest request, CancellationToken cancellationToken)
    {
        LocalAppends.Begin();
        var outcome = ReplicaLocalAppends.Outcome.Ignored;
        try
        {
            var result = await _log.AppendAsync(request, cancellationToken).ConfigureAwait(false);
            outcome = Classify(in result);
            return result;
        }
        catch (Exception error) when (error is not ObjectDisposedException)
        {
            // An append that left the log latched is a refusal that stays: the next write does not reach the log at all.
            outcome = _log.Readiness == FollowerLogReadiness.Failed ? ReplicaLocalAppends.Outcome.Refused : ReplicaLocalAppends.Outcome.Failed;
            throw;
        }
        finally
        {
            LocalAppends.End(outcome);
        }
    }

    /// <summary>Reads the commit index, which the commit path advances while heartbeats and reads run outside the commit gate.</summary>
    /// <returns>The commit index.</returns>
    private ulong ReadCommitIndex() => Volatile.Read(ref _commitIndex);
}
