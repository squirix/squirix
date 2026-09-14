namespace Squirix.Server.Storage.Replication;

/// <summary>Kind of a replica-group journal record that carries idempotency semantics.</summary>
internal enum GroupRecordKind
{
    /// <summary>A user mutation replicated by the leader.</summary>
    UserMutation = 1,

    /// <summary>An internal expiration record produced by the leader.</summary>
    Expiration = 2,

    /// <summary>Metadata that resolves or cancels a prior reservation.</summary>
    Metadata = 3,
}
