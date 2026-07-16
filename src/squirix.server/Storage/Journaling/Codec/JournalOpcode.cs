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
}
