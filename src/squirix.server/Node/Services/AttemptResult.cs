namespace Squirix.Server.Node.Services;

/// <summary>
/// Represents the outcome of a journal compaction attempt.
/// </summary>
internal enum AttemptResult
{
    /// <summary>
    /// The attempt was skipped and no compaction was performed.
    /// </summary>
    Skipped,

    /// <summary>
    /// The compaction completed successfully.
    /// </summary>
    Succeeded,

    /// <summary>
    /// The compaction attempt failed.
    /// </summary>
    Failed,
}
