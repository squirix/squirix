namespace Squirix.Server.Storage.Journaling.Abstractions;

internal static class FilePrefixes
{
    /// <summary>On-disk journal segment filename prefix.</summary>
    internal const string Journal = "jrn-";

    internal const string Manifest = "man-";

    internal const string Snapshot = "snp-";
}
