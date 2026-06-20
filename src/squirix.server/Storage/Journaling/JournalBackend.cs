namespace Squirix.Server.Storage.Journaling;

/// <summary>Selects the journal coordinator implementation (format + write path).</summary>
public enum JournalBackend
{
    /// <summary>Legacy JSON-framed journal (baseline / regression).</summary>
    JsonFramed,

    /// <summary>Single-writer pipelined journal with binary frames.</summary>
    Pipelined,
}
