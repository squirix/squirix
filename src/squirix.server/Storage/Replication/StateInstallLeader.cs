using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>The leader that sends a state transfer.</summary>
/// <param name="Term">Term of the leader; a term below the durable term is refused, a higher one is adopted.</param>
/// <param name="NodeId">Node identifier of the leader.</param>
/// <param name="CeilingChecked">
/// Whether the leader read the current terms of members that, with itself and without this copy, form a majority, and found none above its
/// own. Only then may an unseeded copy take the transfer, and its completion records a vote for the leader in its term.
/// </param>
[Immutable]
internal readonly record struct StateInstallLeader(ulong Term, string NodeId, bool CeilingChecked);
