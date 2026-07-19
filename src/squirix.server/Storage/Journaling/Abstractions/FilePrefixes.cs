namespace Squirix.Server.Storage.Journaling.Abstractions;

internal static class FilePrefixes
{
    /// <summary>On-disk journal segment filename prefix.</summary>
    public const string Journal = "jrn-";

    public const string Manifest = "man-";
    internal const string Snapshot = "snp-";
}
