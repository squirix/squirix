using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Inputs for shared manifest retention cleanup.</summary>
internal readonly struct ManifestRetentionContext
{
    public ManifestRetentionContext(
        string dataDir,
        int manifestRetention,
        int snapshotRetention,
        IStorageFileOperations fileOperations,
        ILogger<ManifestStore>? logger,
        string manifestFileGlob,
        Func<string, int> parseManifestIndex)
    {
        DataDir = dataDir;
        ManifestRetention = manifestRetention;
        SnapshotRetention = snapshotRetention;
        FileOperations = fileOperations;
        Logger = logger;
        ManifestFileGlob = manifestFileGlob;
        ParseManifestIndex = parseManifestIndex;
    }

    public string DataDir { get; }

    public int ManifestRetention { get; }

    public int SnapshotRetention { get; }

    public IStorageFileOperations FileOperations { get; }

    public ILogger<ManifestStore>? Logger { get; }

    public string ManifestFileGlob { get; }

    public Func<string, int> ParseManifestIndex { get; }
}
