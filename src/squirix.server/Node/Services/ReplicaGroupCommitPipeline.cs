using System;
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
/// drives one commit at a time. The local append of N queues N on every follower sender right after it succeeds, with nothing
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
    private readonly ulong _term;
    private ulong _commitIndex;
    private Task<ReplicaDurableAcknowledgement>[] _enqueued = [];
    private ulong _enqueuedIndex;
    private ulong _prevLogIndex;
    private ulong _prevLogTerm;

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
        _term = term;
        _prevLogIndex = status.LastLogIndex;
        _prevLogTerm = status.LastLogTerm;
        _commitIndex = status.CommitIndex;
    }

    /// <summary>Gets the group log index after the last entry this pipeline appended locally, or after the seeded status.</summary>
    /// <remarks>Read by the committer under its gate, between commits, once the commit that last appended has completed.</remarks>
    internal ulong NextLogIndex => _prevLogIndex + 1;

    /// <inheritdoc />
    public async ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
    {
        var result = await _log.AdvanceCommitAsync(commitIndex, cancellationToken).ConfigureAwait(false);
        if (!result.Success)
            throw new InvalidOperationException($"Local group commit advance was refused: {result.RefusalCode}.");

        Volatile.Write(ref _commitIndex, result.CommitIndex);
    }

    /// <inheritdoc />
    /// <remarks>
    /// Returns the acknowledgement task of the follower's sender, which the local append of the same entry already queued it on; the
    /// cancellation token is deliberately ignored, so a late acknowledgement still reaches the observation of the remaining followers.
    /// The commit itself stops at its budget while it waits for the majority.
    /// </remarks>
    /// <exception cref="InvalidOperationException">The entry is not the one this pipeline appended last.</exception>
    public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        mutation.LogIndex == _enqueuedIndex && _enqueuedIndex != 0
            ? new ValueTask<ReplicaDurableAcknowledgement>(_enqueued[_slots.SenderOf(replicaIndex)])
            : ValueTask.FromException<ReplicaDurableAcknowledgement>(
                new InvalidOperationException($"Entry {mutation.LogIndex} was not appended locally by this pipeline; the last one was {_enqueuedIndex}."));

    /// <inheritdoc />
    /// <remarks>
    /// Once the local append succeeded, the entry is queued on every follower sender before anything else runs: queueing never waits
    /// and never throws, so an exception later in the commit cannot leave a locally appended entry unsent.
    /// </remarks>
    public async ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
    {
        if (ReplicaLogCodec.Decode(mutation.CanonicalPayload) is not { } record)
            throw new InvalidOperationException("Prepared mutation carries an undecodable canonical payload.");

        var entries = new FollowerLogEntry[1];
        entries[0] = new FollowerLogEntry(mutation.LogIndex, mutation.Term, mutation.CanonicalPayload);
        var request = new FollowerLogAppendRequest(_selfId, mutation.Term, _prevLogIndex, _prevLogTerm, _commitIndex, new ReadOnlyMemory<FollowerLogEntry>(entries));
        var result = await _log.AppendAsync(request, cancellationToken).ConfigureAwait(false);
        if (!result.Success && string.Equals(result.RefusalCode, FollowerLogRefusal.StaleTerm, StringComparison.Ordinal))
            throw new ReplicaTermSupersededException($"Local group append was refused: the log holds a term above the entry term {mutation.Term}.");
        if (!result.Success)
            throw new InvalidOperationException($"Local group append was refused: {result.RefusalCode}.");

        // The fan-out carries this very entry, so it names the predecessor, not the entry itself.
        var enqueued = new Task<ReplicaDurableAcknowledgement>[_senders.Length];
        for (var i = 0; i < enqueued.Length; i++)
            enqueued[i] = _senders[i].EnqueueAsync(mutation, in record, _prevLogIndex, _prevLogTerm, _commitIndex);

        _enqueued = enqueued;
        _enqueuedIndex = mutation.LogIndex;
        _prevLogIndex = mutation.LogIndex;
        _prevLogTerm = mutation.Term;
    }

    /// <inheritdoc />
    /// <remarks>Every entry the coordinator applies, its own and those a late majority commits, advances the committer's applied index.</remarks>
    public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        _applier.ApplyAsync(mutation.LogIndex, mutation.CanonicalPayload, cancellationToken);

    /// <inheritdoc />
    /// <remarks>Called under the commit gate and from background follower observation; it never waits and never throws.</remarks>
    public void RecordLaggingReplica(int replicaIndex, ulong logIndex) => _lagging.Record(replicaIndex, logIndex, _senders[_slots.SenderOf(replicaIndex)].NodeId);

    /// <summary>Sends one heartbeat, carrying the commit index, to every follower whose sender is idle.</summary>
    /// <remarks>Called outside the commit gate; it never waits and never throws, and a closed sender sends nothing.</remarks>
    internal void Heartbeat()
    {
        var commitIndex = Volatile.Read(ref _commitIndex);
        for (var i = 0; i < _senders.Length; i++)
            _ = _senders[i].TryEnqueueHeartbeat(commitIndex);
    }

    /// <summary>Gets what a catch-up of a follower slot runs against: this pipeline, the slot's sender, the leader log and term.</summary>
    /// <param name="replicaIndex">Zero-based follower slot, never the leader slot.</param>
    /// <returns>The target.</returns>
    internal ReplicaCatchUpTarget CatchUpTargetFor(int replicaIndex) => new(replicaIndex, this, _senders[_slots.SenderOf(replicaIndex)], _log, _term);

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
}
