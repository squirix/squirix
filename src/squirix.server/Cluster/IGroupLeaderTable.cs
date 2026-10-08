namespace Squirix.Server.Cluster;

/// <summary>The leader of every replica group this node serves, as far as this node knows.</summary>
/// <remarks>
/// It changes only with the election state of a group: authority once the leader-term entry of a won term is committed, and a known
/// leader once a follower accepts its contact. A group whose leader is unknown, or whose election does not run, has no route.
/// </remarks>
internal interface IGroupLeaderTable
{
    /// <summary>Tells whether this node leads a group with authority, so it may serve its writes.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="term">The led term when this node has authority; otherwise zero.</param>
    /// <returns><see langword="true" /> when this node leads the group and its leader-term entry is committed.</returns>
    bool HasLocalAuthority(string groupId, out ulong term);

    /// <summary>Reads the leader of a group: this node when it has authority, otherwise the leader it last accepted contact from.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="route">The leader and its term; <see langword="default" /> when none is known.</param>
    /// <returns><see langword="true" /> when a leader is known.</returns>
    bool TryGetLeader(string groupId, out LeaderRoute route);
}
