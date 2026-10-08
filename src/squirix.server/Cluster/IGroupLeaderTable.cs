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

    /// <summary>Reads, in one consistent view, what this node knows of the leader of a group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>
    /// The view; its known leader is this node while it has authority, otherwise the leader it last accepted contact from unless that
    /// route was refuted. <see langword="default" /> (unserved) for a group this node holds no election state for.
    /// </returns>
    GroupLeaderView Read(string groupId);

    /// <summary>Stops reporting a route that answered as stale, until the group reports another one.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="route">The route that answered as stale.</param>
    /// <remarks>Authority of this node is never refuted: the election state alone revokes it.</remarks>
    void Refute(string groupId, in LeaderRoute route);

    /// <summary>Reads the leader of a group: this node when it has authority, otherwise the leader it last accepted contact from.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="route">The leader and its term; <see langword="default" /> when none is known.</param>
    /// <returns><see langword="true" /> when a leader is known.</returns>
    bool TryGetLeader(string groupId, out LeaderRoute route);

    /// <summary>Waits until a leader of a group is known, or <paramref name="timeout" /> elapses.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="timeout">The longest wait; zero or less checks once without waiting.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait by throwing.</param>
    /// <returns><see langword="true" /> when <see cref="TryGetLeader" /> now succeeds; <see langword="false" /> on timeout or for an unserved group.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    ValueTask<bool> WaitForLeaderAsync(string groupId, TimeSpan timeout, CancellationToken cancellationToken);
}
