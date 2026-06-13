namespace Squirix.Server.Storage;

internal static class StorageFilePrefixes
{
    /// <summary>On-disk journal segment filename prefix.</summary>
    public const string Journal = "jrn-";

    /// <summary>Directory glob for journal segment files.</summary>
    public const string JournalSegmentGlob = Journal + "*" + StorageFileExtensions.Journal;

    public const string Manifest = "man-";
    public const string Snapshot = "snp-";
}
