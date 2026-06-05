namespace Squirix.Server.Storage;

/// <summary>
/// Labels for manifest retention cleanup failure outcomes.
/// </summary>
internal static class ManifestRetentionFailureOutcome
{
    public const string CleanupException = "cleanup_exception";
    public const string DeleteFailed = "delete_failed";
}
