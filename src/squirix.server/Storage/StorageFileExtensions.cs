namespace Squirix.Server.Storage;

internal static class StorageFileExtensions
{
    /// <summary>On-disk journal segment file extension.</summary>
    public const string Journal = ".jsqx";

    public const string Manifest = ".msqx";

    /// <summary>On-disk binary manifest file extension.</summary>
    public const string BinaryManifest = ".bmqx";

    public const string Snapshot = ".ssqx";
}
