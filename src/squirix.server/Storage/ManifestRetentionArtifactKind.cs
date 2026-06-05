namespace Squirix.Server.Storage;

/// <summary>
/// Labels for manifest retention cleanup artifact metrics and logs.
/// </summary>
internal static class ManifestRetentionArtifactKind
{
    public const string Manifest = "manifest";

    public const string Snapshot = "snapshot";
    public const string JournalSegment = "journal_segment";
}
