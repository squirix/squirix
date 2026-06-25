namespace Squirix.Server.Storage.Journaling;

/// <summary>Selects the journal coordinator implementation (format + write path).</summary>
public enum JournalBackend
{
    /// <summary>Single-writer pipelined journal with binary frames.</summary>
    Pipelined = 0,
}
