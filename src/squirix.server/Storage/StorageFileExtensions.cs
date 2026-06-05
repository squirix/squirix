namespace Squirix.Server.Storage;

internal static class StorageFileExtensions
{
    public const string Manifest = ".msqx";
    public const string Snapshot = ".ssqx";

    /// <summary>On-disk journal segment file extension.</summary>
    public const string Journal = ".jsqx";
}
