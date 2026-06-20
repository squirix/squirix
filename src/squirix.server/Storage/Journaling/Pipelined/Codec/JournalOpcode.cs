namespace Squirix.Server.Storage.Journaling.Pipelined.Codec;

/// <summary>Binary journal frame opcodes.</summary>
internal enum JournalOpcode : byte
{
    /// <summary>Put operation.</summary>
    Put = 1,

    /// <summary>Remove operation.</summary>
    Remove = 2,

    /// <summary>Remove expiration operation.</summary>
    RemoveExpiration = 3,

    /// <summary>Touch expiration operation.</summary>
    TouchExpiration = 4,
}
