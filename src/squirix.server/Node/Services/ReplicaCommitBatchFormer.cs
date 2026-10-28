using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Node.Services;

/// <summary>Forms one commit batch from the leading write and the writes queued behind it, under the commit gate.</summary>
/// <remarks>
/// <para>
/// Each write is taken from the head of the queue and prepared at the next consecutive log index. A batch holds each cache key and each operation
/// identity once, and no identity the group already retains: a prepare reads only its own key and the clock, so entries of distinct keys decide the same as if
/// they were committed one by one. The write that would repeat a key or an identity ends the batch and stays queued, as does one that would no longer fit
/// the payload bytes or the idempotency capacity. A write whose identity is retained is committed alone, where the coordinator answers a retry from the record.
/// </para>
/// <para>
/// A write whose prepare fails is refused alone and takes no index. The budget ending ends the batch where it stands: the write being prepared is refused and
/// the prepared ones are committed.
/// </para>
/// </remarks>
[Mutable]
internal sealed class ReplicaCommitBatchFormer
{
    private readonly ReplicaGroupCommitter _committer;
    private readonly ReplicaMutationFactory _factory;
    private readonly IFollowerLog? _log;
    private long _bytes;
    private long _chargedBytes;
    private int _chargedRecords;
    private HashSet<(string Scope, string Key)>? _keys;
    private HashSet<(string Scope, string OperationId)>? _operations;
    private List<ReplicaCommitRequest>? _taken;

    /// <summary>Initializes a new instance of the <see cref="ReplicaCommitBatchFormer" /> class.</summary>
    /// <param name="committer">The committer of the group, whose commit gate the caller holds.</param>
    /// <param name="factory">The running mutation factory.</param>
    internal ReplicaCommitBatchFormer(ReplicaGroupCommitter committer, ReplicaMutationFactory factory)
    {
        ArgumentNullException.ThrowIfNull(committer);
        ArgumentNullException.ThrowIfNull(factory);
        _committer = committer;
        _factory = factory;
        _log = committer.Registry.TryGetLog(committer.GroupId, out var log) ? log : null;
    }

    private enum Admission
    {
        Take = 0,
        Stop = 1,
        Skip = 2,
    }

    /// <summary>Gets the writes of the batch with their prepared mutations, in log order.</summary>
    internal List<(ReplicaCommitRequest Request, PreparedReplicaMutation Mutation)> Entries { get; } = [];

    /// <summary>Gets the writes the former took from the queue behind the leader, so the caller can decide the ones a fault leaves open.</summary>
    internal IReadOnlyList<ReplicaCommitRequest> Taken => _taken ?? [];

    /// <summary>Takes the queued writes into the batch of the leader.</summary>
    /// <param name="self">The leading write, already taken, which opens the batch.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    /// <returns>An asynchronous operation.</returns>
    internal async Task FormAsync(ReplicaCommitRequest self, CancellationToken cancellationToken)
    {
        var firstIndex = _committer.PeekNextIndex();
        ReplicaCommitRequest? candidate = self;
        var queued = false;
        while (candidate != null)
        {
            var admission = queued ? Admit(candidate) : Admission.Take;
            if (admission == Admission.Stop)
                return;

            if (admission == Admission.Skip)
            {
                candidate = _committer.Combiner.PeekLive();
                continue;
            }

            if (!await DecideAsync(candidate, queued, firstIndex + ulong.CreateChecked(Entries.Count), cancellationToken).ConfigureAwait(false))
                return;

            candidate = _committer.Combiner.PeekLive();
            queued = true;
        }
    }

    private static long ChargeOf(PreparedReplicaMutation mutation) => GroupIdempotencyState.ReservationCharge(
        GroupRecordKinds.FromScope(mutation.OperationScope),
        mutation.OperationScope,
        mutation.OperationId,
        mutation.OperationFingerprint.Length,
        mutation.OutcomePayload.Length);

    /// <summary>Takes the head of the queue into the batch, unless the batch is full or the head repeats a key or an identity of it.</summary>
    /// <param name="candidate">The head of the queue.</param>
    /// <returns>Whether the head is taken, ends the batch, or was canceled meanwhile.</returns>
    private Admission Admit(ReplicaCommitRequest candidate)
    {
        if (Entries.Count >= ReplicaGroupCommitterBatches.MaxBatchEntries || _bytes >= ReplicaGroupCommitterBatches.MaxBatchBytes)
            return Admission.Stop;

        if (RepeatsEntry(candidate))
            return Admission.Stop;

        if (!candidate.TryTake())
            return Admission.Skip;

        (_taken ??= []).Add(candidate);
        return Admission.Take;
    }

    /// <summary>Prepares a taken write and adds it to the batch, or hands it back to the queue when it cannot join.</summary>
    /// <param name="candidate">The taken write.</param>
    /// <param name="queued">Whether the write is still the head of the queue.</param>
    /// <param name="index">The log index of the write in the batch.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    /// <returns><see langword="true" /> when the batch can take another write.</returns>
    private async Task<bool> DecideAsync(ReplicaCommitRequest candidate, bool queued, ulong index, CancellationToken cancellationToken)
    {
        var retained = _log != null && _log.Idempotency.Lookup(candidate.Scope, candidate.OperationId, [], out _) != GroupIdempotencyLookup.Miss;
        if (retained && Entries.Count > 0)
        {
            Return(candidate);
            return false;
        }

        var (mutation, ended) = await PrepareAsync(candidate, index, cancellationToken).ConfigureAwait(false);
        var more = !ended;
        if (mutation != null)
        {
            if (!TryAdd(candidate, mutation))
            {
                Return(candidate);
                return false;
            }

            more = more && !retained && FitsAnother();
        }

        if (queued)
            _committer.Combiner.RemoveHead(candidate);

        return more;
    }

    /// <summary>Tells whether the idempotency capacity and the payload bytes still hold the entries of the batch.</summary>
    /// <returns><see langword="true" /> when the batch can take another write.</returns>
    private bool FitsAnother() => Fits(0L, 0);

    private bool Fits(long charge, int payloadBytes) =>
        _bytes + payloadBytes <= ReplicaGroupCommitterBatches.MaxBatchBytes
        && (_log?.Idempotency.HasRoomFor(_chargedRecords + (charge > 0 ? 1 : 0), _chargedBytes + charge) ?? true);

    /// <summary>Prepares the mutation of a taken write; a failure refuses the write alone.</summary>
    /// <param name="candidate">The taken write.</param>
    /// <param name="index">The log index of the write in the batch.</param>
    /// <param name="cancellationToken">The commit budget token.</param>
    /// <returns>The mutation, or <see langword="null" /> when the write is refused, and whether the budget ended the batch.</returns>
    [SuppressMessage("Design", "CA1031", Justification = "A fault of the prepare is recorded as the failure of that write alone, whose caller rethrows it.")]
    private async Task<(PreparedReplicaMutation? Mutation, bool Ended)> PrepareAsync(ReplicaCommitRequest candidate, ulong index, CancellationToken cancellationToken)
    {
        try
        {
            return (await candidate.PrepareAsync(_factory, index, cancellationToken).ConfigureAwait(false), false);
        }
        catch (ObjectDisposedException)
        {
            // The prepare only reads the local cache, so a cache disposed under it leaves nothing of the write read into the log.
            candidate.Fail(ServerOpContract.NoLeaderAuthority());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            candidate.Fail(ServerOpContract.TooManyRequests(ReplicaGroupCommitter.CommitBudgetRefusalReason));
            return (null, true);
        }
        catch (Exception error)
        {
            // Nothing of this write reached the batch, so it fails alone and takes no index.
            candidate.Fail(error);
        }

        return (null, false);
    }

    /// <summary>Hands a taken write back to the head of the queue; a disposal that took it from the queue meanwhile leaves its decision here.</summary>
    /// <param name="candidate">The write, which is the last one taken.</param>
    private void Return(ReplicaCommitRequest candidate)
    {
        _taken!.RemoveAt(_taken.Count - 1);
        if (!_committer.Combiner.Restore(candidate))
            _committer.Refuse(candidate, ServerOpContract.NoLeaderAuthority());
    }

    /// <summary>Tells whether the batch already holds the cache key or the operation identity of a write.</summary>
    /// <param name="candidate">The write.</param>
    /// <returns><see langword="true" /> when the write repeats either.</returns>
    private bool RepeatsEntry(ReplicaCommitRequest candidate)
    {
        // A batch of one entry compares directly; the sets exist only once a second write joined.
        if (_keys == null || _operations == null)
        {
            if (Entries.Count == 0)
                return false;

            var only = Entries[0].Request;
            return string.Equals(candidate.Scope, only.Scope, StringComparison.Ordinal)
                   && (string.Equals(candidate.Key, only.Key, StringComparison.Ordinal) || string.Equals(candidate.OperationId, only.OperationId, StringComparison.Ordinal));
        }

        return _keys.Contains((candidate.Scope, candidate.Key)) || _operations.Contains((candidate.Scope, candidate.OperationId));
    }

    /// <summary>Adds a prepared write to the batch, unless it would not fit.</summary>
    /// <param name="candidate">The write.</param>
    /// <param name="mutation">Its prepared mutation.</param>
    /// <returns><see langword="true" /> when added; the first write of a batch is always added.</returns>
    private bool TryAdd(ReplicaCommitRequest candidate, PreparedReplicaMutation mutation)
    {
        var charge = ChargeOf(mutation);
        var payloadBytes = mutation.CanonicalPayload.Length;
        if (Entries.Count > 0 && !Fits(charge, payloadBytes))
            return false;

        Entries.Add((candidate, mutation));
        if (Entries.Count > 1)
        {
            if (_keys == null || _operations == null)
            {
                _keys = [];
                _operations = [];
                _ = _keys.Add((Entries[0].Request.Scope, Entries[0].Request.Key));
                _ = _operations.Add((Entries[0].Request.Scope, Entries[0].Request.OperationId));
            }

            _ = _keys.Add((candidate.Scope, candidate.Key));
            _ = _operations.Add((candidate.Scope, candidate.OperationId));
        }

        _bytes += payloadBytes;
        if (charge > 0)
        {
            _chargedRecords++;
            _chargedBytes += charge;
        }

        return true;
    }
}
