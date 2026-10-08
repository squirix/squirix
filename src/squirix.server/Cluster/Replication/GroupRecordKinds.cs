using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Maps the operation scope of a replicated record to the kind of its idempotency record.</summary>
internal static class GroupRecordKinds
{
    /// <summary>Returns the record kind of an operation scope.</summary>
    /// <param name="scope">The operation scope of the record.</param>
    /// <returns>
    /// <see cref="GroupRecordKind.Expiration" /> for the expiration scope, <see cref="GroupRecordKind.LeaderTerm" /> for the leader-term
    /// scope, and <see cref="GroupRecordKind.UserMutation" /> for any other scope, which is a cache name.
    /// </returns>
    internal static GroupRecordKind FromScope(string scope) => scope switch
    {
        ReplicaExpirationOperationId.OperationScope => GroupRecordKind.Expiration,
        ReplicaLeaderOperationId.OperationScope => GroupRecordKind.LeaderTerm,
        _ => GroupRecordKind.UserMutation,
    };
}
