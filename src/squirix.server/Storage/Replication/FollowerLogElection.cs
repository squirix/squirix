using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Election voting operations for a follower log.</summary>
/// <remarks>
/// The caller holds the log gate; term and vote transitions persist durably through the owner before
/// any granted result is reported, so a restart never observes a phantom term or a second vote. A grant at a higher term stores the
/// term and the vote as one record, and a refusal at a higher term stores the term alone. Either record is mirrored into both slots, and the
/// result is reported after the first copy is durable: the mirror copy follows in the same work item and stays pending until the next
/// metadata publish or the dispose waits for it.
/// </remarks>
internal static class FollowerLogElection
{
    internal static FollowerLogVoteResult CheckPreVote(FollowerLogJournal journal, IFollowerLogContext owner, in ElectionVoteRequest request)
    {
        // A pre-vote probe never steps the term: an isolated follower soliciting probes must not inflate
        // its durable term, and the reported term always stays the locally persisted one.
        return true switch
        {
            _ when request.Term < owner.Meta.CurrentTerm => new FollowerLogVoteResult(false, FollowerLogRefusal.StaleTerm, owner.Meta.CurrentTerm),
            _ when request.Term == owner.Meta.CurrentTerm && owner.Meta.VotedFor.Length != 0 &&
                   !string.Equals(owner.Meta.VotedFor, request.CandidateId, StringComparison.Ordinal) => new FollowerLogVoteResult(
                false,
                FollowerLogRefusal.AlreadyVoted,
                owner.Meta.CurrentTerm),
            _ when !FollowerLogAppend.IsLogUpToDate(request.LastLogTerm, request.LastLogIndex, CurrentLastLogTerm(journal, owner), owner.LastLogIndex) =>
                new FollowerLogVoteResult(false, FollowerLogRefusal.StaleLog, owner.Meta.CurrentTerm),
            _ => new FollowerLogVoteResult(true, string.Empty, owner.Meta.CurrentTerm),
        };
    }

    internal static async Task<FollowerLogVoteResult> RequestVoteAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        ElectionVoteRequest request,
        CancellationToken cancellationToken)
    {
        // A stale candidate term authorizes nothing and never touches durable state.
        if (request.Term < owner.Meta.CurrentTerm)
            return new FollowerLogVoteResult(false, FollowerLogRefusal.StaleTerm, owner.Meta.CurrentTerm);

        // The checks run against the state a higher term would step to, before anything is written: the step clears the previous vote, so a
        // higher term can never be refused as already voted, and a grant at a higher term stores the term and the vote as one record.
        var higherTerm = request.Term > owner.Meta.CurrentTerm;
        var votedFor = higherTerm ? string.Empty : owner.Meta.VotedFor;

        // At most one vote per term: only the recorded candidate may be re-granted.
        if (votedFor.Length != 0 && !string.Equals(votedFor, request.CandidateId, StringComparison.Ordinal))
            return new FollowerLogVoteResult(false, FollowerLogRefusal.AlreadyVoted, owner.Meta.CurrentTerm);

        // A candidate whose log trails the voter log cannot win the election. A higher term is still stored on its own: the voter has
        // seen it, and a crash must not forget it.
        if (!FollowerLogAppend.IsLogUpToDate(request.LastLogTerm, request.LastLogIndex, CurrentLastLogTerm(journal, owner), owner.LastLogIndex))
        {
            if (higherTerm)
                await StoreAsync(journal, owner, owner.Meta with { CurrentTerm = request.Term, VotedFor = string.Empty }, cancellationToken).ConfigureAwait(false);

            return new FollowerLogVoteResult(false, FollowerLogRefusal.StaleLog, owner.Meta.CurrentTerm);
        }

        // A replayed grant for the recorded candidate needs no durable rewrite: the vote was
        // persisted before the first grant was reported, so the file already records it.
        if (!higherTerm && string.Equals(votedFor, request.CandidateId, StringComparison.Ordinal))
            return new FollowerLogVoteResult(true, string.Empty, owner.Meta.CurrentTerm);

        // The granted vote is persisted before reporting success, so a restart never grants a second vote. This is also the path of a
        // candidate's vote for itself. Once a higher term is going to be stored it is part of the log whatever happens next; the one record
        // is scheduled under the caller's token and, once its first copy is durable, the vote runs to completion.
        await StoreAsync(journal, owner, owner.Meta with { CurrentTerm = request.Term, VotedFor = request.CandidateId }, cancellationToken).ConfigureAwait(false);
        return new FollowerLogVoteResult(true, string.Empty, owner.Meta.CurrentTerm);
    }

    /// <summary>Persists a vote record mirrored into both slots and makes it the metadata of the log once its first copy is durable.</summary>
    /// <param name="journal">The storage surface.</param>
    /// <param name="owner">The owning follower log.</param>
    /// <param name="candidate">The metadata to store in both slots.</param>
    /// <param name="cancellationToken">Cancellation token for the wait and the scheduling.</param>
    /// <returns>A task that completes once the first copy is durable and the metadata is current.</returns>
    /// <remarks>
    /// The mirror copy follows right after in the same work item and stays pending on the owner, so the reply waits for one flush. A damaged
    /// newest slot would otherwise roll a term or a vote back to the previous record, which is why the record is mirrored at all.
    /// </remarks>
    private static async Task StoreAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupLogMetadata candidate, CancellationToken cancellationToken)
    {
        await FollowerLogAppend.PersistDeferredMirrorOrFailAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
    }

    private static ulong CurrentLastLogTerm(FollowerLogJournal journal, IFollowerLogContext owner)
    {
        return true switch
        {
            _ when owner.LastLogIndex == 0UL => 0UL,
            _ when journal.TryGetEntryOffset(owner.LastLogIndex, out var location) => location.Term,
            _ when journal.SnapshotBaseline.LastIncludedIndex == owner.LastLogIndex => journal.SnapshotBaseline.LastIncludedTerm,
            _ => 0UL,
        };
    }
}
