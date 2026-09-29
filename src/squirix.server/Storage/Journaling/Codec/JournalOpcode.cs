namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Binary journal frame opcodes.</summary>
/// <remarks>
/// Wire values 3, 4, 8 and 9 belonged to the retired touch-expiration and remove-expiration delta frames and are never reused. A frame that
/// carries a retired or unassigned opcode fails recovery.
/// </remarks>
internal enum JournalOpcode
{
    /// <summary>Put operation.</summary>
    Put = 1,

    /// <summary>Remove operation.</summary>
    Remove = 2,

    /// <summary>Idempotency outcome record (operation id + fingerprint + response bytes).</summary>
    IdempotencyOutcome = 5,

    /// <summary>Put operation carrying the write-ahead mutation operation id prefix.</summary>
    PutWithMutationOperationId = 6,

    /// <summary>Remove the operation carrying the write-ahead mutation operation id prefix.</summary>
    RemoveWithMutationOperationId = 7,

    /// <summary>Write-ahead idempotency intent (operation id + fingerprint, no response bytes).</summary>
    IdempotencyStarted = 10,
}
