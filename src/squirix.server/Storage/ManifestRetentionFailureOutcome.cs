namespace Squirix.Server.Storage;

/// <summary>Labels for manifest retention cleanup failure outcomes.</summary>
internal static class ManifestRetentionFailureOutcome
{
    public const string CleanupException = "cleanup_exception";
    internal const string DeleteFailed = "delete_failed";
}
