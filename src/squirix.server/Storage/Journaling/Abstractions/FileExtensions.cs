namespace Squirix.Server.Storage.Journaling.Abstractions;

internal static class FileExtensions
{
    /// <summary>On-disk journal segment file extension.</summary>
    public const string Journal = ".jsqx";

    /// <summary>On-disk manifest file extension.</summary>
    public const string Manifest = ".bmqx";

    /// <summary>On-disk binary snapshot file extension.</summary>
    public const string Snapshot = ".bsqx";
}
