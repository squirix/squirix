using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Read-specific inputs for a leader authority check.</summary>
/// <param name="QuorumConfirmed">Whether a quorum confirmed leadership for the current read.</param>
/// <param name="AppliedIndex">The locally applied index.</param>
/// <param name="ReadIndex">The read index the read must observe.</param>
[Immutable]
internal readonly record struct LeaderReadState(bool QuorumConfirmed, ulong AppliedIndex, ulong ReadIndex);
