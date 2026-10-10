using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster;

/// <summary>The leader of every replica group this node serves, as far as this node knows.</summary>
/// <remarks>
/// When elections lead the groups, it changes only with the election state of a group: authority once the leader-term entry of a won
/// term is committed, and a known leader once a follower accepts its contact; a group whose leader is unknown has no route. When no
/// election runs, every group is led statically by its ring owner.
/// </remarks>
internal interface IGroupLeaderTable
{
    /// <summary>Tells whether this node leads a group with authority, so it may serve its writes.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="term">The led term when this node has authority; otherwise zero.</param>
    /// <returns><see langword="true" /> when this node leads the group and its leader-term entry is committed.</returns>
    bool HasLocalAuthority(string groupId, out ulong term);

    /// <summary>Keeps the leader a peer named for a group this node does not serve, so later calls of that group go to it first.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="hint">The leader a stale refusal named, or the member that served a call; its term is zero when unknown.</param>
    /// <remarks>
    /// One hint is kept per group; a hint of a lower term than the kept one is ignored. A hint for a served group, or naming a node outside the
    /// replica set of the group, is ignored: a served group follows its election state alone. A table without elections keeps no hint.
    /// </remarks>
    void Learn(string groupId, in LeaderRoute hint);

    /// <summary>Reads, in one consistent view, what this node knows of the leader of a group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>
    /// The view; its known leader is this node while it has authority, otherwise the leader it last accepted contact from within one
    /// election timeout, unless that route was refuted. <see langword="default" /> (unserved) for a group this node holds no election state for.
    /// </returns>
    GroupLeaderView Read(string groupId);

    /// <summary>Stops reporting a route that answered as stale, and any known leader of its term or below, until a higher term is known.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="route">The route that answered as stale, or could not be reached.</param>
    /// <remarks>
    /// Of several refutations the one of the highest term is kept. Authority of this node is never refuted: the election state alone
    /// revokes it. For a group this node does not serve, it forgets the learned leader when that names the same node in the term of the
    /// route or below.
    /// </remarks>
    void Refute(string groupId, in LeaderRoute route);

    /// <summary>Reads the leader of a group: this node when it has authority, otherwise the leader it last accepted contact from.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="route">The leader and its term; <see langword="default" /> when none is known.</param>
    /// <returns><see langword="true" /> when a leader is known.</returns>
    /// <remarks>
    /// Refuted routes are hidden here as in <see cref="Read" />, so no caller, the stale-owner refusal of a write included, names a route that
    /// already answered as stale; such a caller sees no known leader instead. A leader not heard from for one election timeout is hidden the
    /// same way until a contact revives it.
    /// </remarks>
    bool TryGetLeader(string groupId, out LeaderRoute route);

    /// <summary>Reads the leader learned for a group this node does not serve.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="route">The learned leader; <see langword="default" /> when none is kept.</param>
    /// <returns><see langword="true" /> when a leader is kept for the group.</returns>
    bool TryGetLearnedLeader(string groupId, out LeaderRoute route);

    /// <summary>Waits until a leader of a group is known, or <paramref name="timeout" /> elapses.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="timeout">
    /// The longest wait; <see cref="Timeout.InfiniteTimeSpan" /> waits until a leader is known or the wait is canceled; any other value of
    /// zero or less checks once without waiting.
    /// </param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait by throwing.</param>
    /// <returns><see langword="true" /> when <see cref="TryGetLeader" /> now succeeds; <see langword="false" /> on timeout or for an unserved group.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    ValueTask<bool> WaitForLeaderAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken);
}
