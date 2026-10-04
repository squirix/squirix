namespace Squirix.Server.Benchmarks;

/// <summary>The journal segment call a simulated disk stall blocks.</summary>
internal enum JournalStallKind
{
    /// <summary>No call is blocked.</summary>
    None = 0,

    /// <summary>Blocks FlushToDisk, as an fsync that hangs.</summary>
    Flush = 1,

    /// <summary>Blocks Write, as a frozen filesystem.</summary>
    Write = 2,
}
