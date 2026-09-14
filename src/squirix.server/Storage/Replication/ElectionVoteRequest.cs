using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>A ballot request carried by the election path for one replica group.</summary>
/// <param name="CandidateId">The node identifier soliciting the vote.</param>
/// <param name="Term">The candidate term soliciting the vote.</param>
/// <param name="LastLogIndex">The candidate durable last log index.</param>
/// <param name="LastLogTerm">The term at the candidate durable last log index.</param>
[Immutable]
internal readonly record struct ElectionVoteRequest(string CandidateId, ulong Term, ulong LastLogIndex, ulong LastLogTerm);
