using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The leader side of a replica group, which the election driver hands a won term to and takes it back from.</summary>
/// <remarks>
/// Implemented by the node services that commit; the driver never commits itself. Every call may be retried: a promotion that has not
/// reported its leader-term entry committed is called again in the same term, and a retirement that could not finish is called again.
/// </remarks>
internal interface IReplicaLeadership
{
    /// <summary>Leads a group in a won term and commits its leader-term entry.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="term">The won term; every entry the leader appends carries it.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// <see langword="true" /> once the leader-term entry of <paramref name="term" /> appended by this leadership is committed by a
    /// majority; <see langword="false" /> while it is not yet, so the caller retries.
    /// </returns>
    Task<bool> PromoteAsync(string groupId, ulong term, CancellationToken cancellationToken);

    /// <summary>Stops leading a group: drains what the leader appended, closes its followers, and hands the group back to its apply loop.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns><see langword="true" /> once the group is retired; <see langword="false" /> while a committed entry is still to be applied.</returns>
    /// <remarks>The caller revokes the authority of the group before it calls, so no write is admitted meanwhile.</remarks>
    Task<bool> RetireAsync(string groupId, CancellationToken cancellationToken);

    /// <summary>Sends one heartbeat to every idle follower of a led group; their replies are posted to the election state of the group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes once the heartbeats are queued; it does not wait for the replies.</returns>
    Task HeartbeatAsync(string groupId, CancellationToken cancellationToken);
}
