using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Identifies an operation by its cache scope and client operation identifier.</summary>
/// <param name="Scope">The cache scope of the operation.</param>
/// <param name="OperationId">The client operation identifier.</param>
[Immutable]
internal readonly record struct ReplicaOperationKey(string Scope, string OperationId);
