using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Manifest;

[Immutable]
internal sealed record RetentionSettings
{
    internal RetentionSettings(string dataDir, int manifestRetention, string manifestFileGlob)
    {
        DataDir = dataDir;
        ManifestRetention = manifestRetention;
        ManifestFileGlob = manifestFileGlob;
    }

    internal string DataDir { get; }

    internal string ManifestFileGlob { get; }

    internal int ManifestRetention { get; }
}
