namespace Squirix.Server.Storage.Journaling.Codec;

/// <summary>Binary journal frame opcodes.</summary>
/// <remarks>The values are the on-disk byte and are sequential from 1; a frame that carries an unassigned value fails decoding.</remarks>
internal enum JournalOpcode
{
    /// <summary>Put operation.</summary>
    Put = 1,

    /// <summary>Remove operation.</summary>
    Remove = 2,

    /// <summary>Idempotency outcome record (operation id + fingerprint + response bytes).</summary>
    IdempotencyOutcome = 3,

    /// <summary>Put operation carrying the write-ahead mutation operation id prefix.</summary>
    PutWithMutationOperationId = 4,

    /// <summary>Remove the operation carrying the write-ahead mutation operation id prefix.</summary>
    RemoveWithMutationOperationId = 5,

    /// <summary>Write-ahead idempotency intent (operation id + fingerprint, no response bytes).</summary>
    IdempotencyStarted = 6,
}
