namespace Squirix.Server.Node.Services;

/// <summary>Represents the outcome of a journal compaction attempt.</summary>
internal enum AttemptResult
{
    /// <summary>The attempt was skipped and no compaction was performed.</summary>
    Skipped = 0,

    /// <summary>The compaction completed successfully.</summary>
    Succeeded = 1,

    /// <summary>The compaction attempt failed.</summary>
    Failed = 2,
}
