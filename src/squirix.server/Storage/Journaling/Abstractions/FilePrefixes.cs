namespace Squirix.Server.Storage.Journaling.Abstractions;

internal static class FilePrefixes
{
    /// <summary>On-disk journal segment filename prefix.</summary>
    public const string Journal = "jrn-";

    public const string Manifest = "man-";

    /// <summary>Zero-padded decimal format for journal and snapshot segment indexes in filenames.</summary>
    internal const string SegmentIndexFormat = "000000";

    internal const string Snapshot = "snp-";
}
