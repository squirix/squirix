namespace Squirix.Server.Storage.Manifest;

internal sealed record RetentionSettings
{
    internal RetentionSettings(string dataDir, int manifestRetention, int snapshotRetention, string manifestFileGlob)
    {
        DataDir = dataDir;
        ManifestRetention = manifestRetention;
        SnapshotRetention = snapshotRetention;
        ManifestFileGlob = manifestFileGlob;
    }

    internal string DataDir { get; }

    internal string ManifestFileGlob { get; }

    internal int ManifestRetention { get; }

    internal int SnapshotRetention { get; }
}
