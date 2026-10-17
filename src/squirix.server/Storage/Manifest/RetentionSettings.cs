using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Manifest;

[Immutable]
internal sealed record RetentionSettings
{
    internal RetentionSettings(string dataDir, string manifestFileGlob)
    {
        DataDir = dataDir;
        ManifestFileGlob = manifestFileGlob;
    }

    internal string DataDir { get; }

    internal string ManifestFileGlob { get; }
}
