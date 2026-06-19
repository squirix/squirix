namespace Squirix.Server.Storage.Journaling.PipelinedWal;

/// <summary>Selects the journal coordinator implementation (format + write path).</summary>
public enum JournalBackend
{
    /// <summary>Legacy JSON-framed WAL (baseline / regression).</summary>
    JsonFramed,

    /// <summary>Single-writer pipelined WAL with binary frames.</summary>
    PipelinedWal,
}
