using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Manifest;

[Immutable]
internal sealed record RetentionSettings
{
    internal RetentionSettings(string dataDir)
    {
        DataDir = dataDir;
    }

    internal string DataDir { get; }
}
