namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Binary journal frame opcodes.</summary>
internal enum JournalOpcode
{
    /// <summary>Put operation.</summary>
    Put = 1,

    /// <summary>Remove operation.</summary>
    Remove = 2,

    /// <summary>Remove expiration operation.</summary>
    RemoveExpiration = 3,

    /// <summary>Touch expiration operation.</summary>
    TouchExpiration = 4,

    /// <summary>Idempotency outcome record (operation id + fingerprint + response bytes).</summary>
    IdempotencyOutcome = 5,

    /// <summary>Put operation carrying the write-ahead mutation operation id prefix.</summary>
    PutWithMutationOperationId = 6,

    /// <summary>Remove the operation carrying the write-ahead mutation operation id prefix.</summary>
    RemoveWithMutationOperationId = 7,

    /// <summary>Remove the expiration operation carrying the write-ahead mutation operation id prefix.</summary>
    RemoveExpirationWithMutationOperationId = 8,

    /// <summary>Touch expiration operation carrying the write-ahead mutation operation id prefix.</summary>
    TouchExpirationWithMutationOperationId = 9,

    /// <summary>Write-ahead idempotency intent (operation id + fingerprint, no response bytes).</summary>
    IdempotencyStarted = 10,
}
