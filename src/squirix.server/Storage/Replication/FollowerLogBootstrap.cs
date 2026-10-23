using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Bootstrap operations of a follower log: the offer, and the completion of an unseeded copy.</summary>
/// <remarks>
/// The caller holds the log gate. Every record is published to both metadata slots through the mirrored path, which fails readiness on a
/// fault like every other metadata write. A crash between the two slot writes leaves a legal state either way: after the first copy of the
/// complete record a rollback to the offered record completes again on the next member call, and nothing was acknowledged in between, because
/// that call is processed only after both copies are durable.
/// </remarks>
internal static class FollowerLogBootstrap
{
    internal static async Task<string?> AcceptOfferAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        bool open,
        bool topologyMismatch,
        CancellationToken cancellationToken)
    {
        var meta = owner.Meta;
        if (!open || meta.MemberState != GroupMemberState.Unseeded)
            return FollowerLogRefusal.NotReady;

        if (topologyMismatch)
            return FollowerLogRefusal.TopologyMismatch;

        // Published to both slots even when the copy holds the offer: a crash between the two slot writes of an earlier offer left it in one.
        await StoreAsync(journal, owner, meta with { BootstrapOffered = true }, cancellationToken).ConfigureAwait(false);
        return null;
    }

    internal static async Task CompleteOwnerAsync(FollowerLogJournal journal, IFollowerLogContext owner, bool open, CancellationToken cancellationToken)
    {
        var meta = owner.Meta;
        if (!open || meta.MemberState != GroupMemberState.Unseeded)
            throw new InvalidOperationException($"Replica group '{owner.GroupId}' copy is not an open unseeded copy, so it cannot be completed as the owner.");

        await StoreAsync(journal, owner, meta with { MemberState = GroupMemberState.Complete }, cancellationToken).ConfigureAwait(false);
    }

    internal static async Task<string?> CompleteIfOfferedAsync(
        FollowerLogJournal journal,
        IFollowerLogContext owner,
        bool open,
        bool topologyMismatch,
        CancellationToken cancellationToken)
    {
        var meta = owner.Meta;
        if (!open)
            return FollowerLogRefusal.NotReady;

        if (topologyMismatch)
            return FollowerLogRefusal.TopologyMismatch;

        if (meta.MemberState == GroupMemberState.Complete)
            return null;

        if (meta.MemberState != GroupMemberState.Unseeded || !meta.BootstrapOffered)
            return NotCompleteRefusal(meta.MemberState);

        await StoreAsync(journal, owner, meta with { MemberState = GroupMemberState.Complete }, cancellationToken).ConfigureAwait(false);
        return null;
    }

    /// <summary>Gets the refusal marker of a copy that is not complete.</summary>
    /// <param name="state">The member state of the copy.</param>
    /// <returns><see cref="FollowerLogRefusal.Incomplete" /> for an incomplete copy, otherwise <see cref="FollowerLogRefusal.Unseeded" />.</returns>
    internal static string NotCompleteRefusal(GroupMemberState state) => state == GroupMemberState.Incomplete ? FollowerLogRefusal.Incomplete : FollowerLogRefusal.Unseeded;

    private static async Task StoreAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupLogMetadata candidate, CancellationToken cancellationToken)
    {
        await FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken).ConfigureAwait(false);
        owner.Meta = candidate;
    }
}
