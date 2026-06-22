namespace Squirix.Server.Storage;

internal static class StorageFileExtensions
{
    /// <summary>On-disk binary snapshot file extension.</summary>
    public const string BinarySnapshot = ".bsqx";

    /// <summary>On-disk journal segment file extension.</summary>
    public const string Journal = ".jsqx";

    /// <summary>On-disk manifest file extension.</summary>
    public const string Manifest = ".bmqx";

    public const string Snapshot = ".ssqx";
}
