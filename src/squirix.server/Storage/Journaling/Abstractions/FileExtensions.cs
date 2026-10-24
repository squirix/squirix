namespace Squirix.Server.Storage.Journaling.Abstractions;

internal static class FileExtensions
{
    /// <summary>On-disk journal segment file extension.</summary>
    public const string Journal = ".jsqx";

    /// <summary>On-disk binary snapshot file extension.</summary>
    public const string Snapshot = ".bsqx";

    /// <summary>Extension of the temp file a journal segment or snapshot is staged in before it is published.</summary>
    internal const string Temp = ".tmp";
}
