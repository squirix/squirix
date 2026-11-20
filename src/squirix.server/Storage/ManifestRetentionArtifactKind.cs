namespace Squirix.Server.Storage;

/// <summary>Labels for manifest retention cleanup artifact metrics and logs.</summary>
internal static class ManifestRetentionArtifactKind
{
    internal const string JournalSegment = "journal_segment";

    internal const string Snapshot = "snapshot";
}
