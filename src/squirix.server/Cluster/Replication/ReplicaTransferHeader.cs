using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Who sends a state transfer and under which identity.</summary>
/// <param name="GroupId">Replica group identifier.</param>
/// <param name="TransferId">The identifier every message of the transfer carries.</param>
/// <param name="LeaderNodeId">The node that sends the state; it must be a member of the group.</param>
/// <param name="LeaderTerm">The term of the sender; a term above the held transfer takes the group over.</param>
[Immutable]
internal readonly record struct ReplicaTransferHeader(string GroupId, string TransferId, string LeaderNodeId, ulong LeaderTerm);
