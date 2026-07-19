using System;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Inputs for shared manifest retention cleanup.</summary>
internal sealed record RetentionContext
{
    internal RetentionContext(
        RetentionSettings settings,
        IStorageFileOperations? fileOperations,
        ILogger? logger,
        Func<string, int> parseManifestIndex,
        IManifestRetentionFailureMetrics failureMetrics)
        : this(
            settings.DataDir,
            settings.ManifestRetention,
            settings.SnapshotRetention,
            fileOperations,
            logger,
            settings.ManifestFileGlob,
            parseManifestIndex,
            failureMetrics ?? throw new ArgumentNullException(nameof(failureMetrics)))
    {
    }

    private RetentionContext(
        string dataDir,
        int manifestRetention,
        int snapshotRetention,
        IStorageFileOperations? fileOperations,
        ILogger? logger,
        string manifestFileGlob,
        Func<string, int> parseManifestIndex,
        IManifestRetentionFailureMetrics failureMetrics)
    {
        DataDir = dataDir;
        ManifestRetention = manifestRetention;
        SnapshotRetention = snapshotRetention;
        FileOperations = fileOperations ?? new FileOperations();
        Logger = logger;
        ManifestFileGlob = manifestFileGlob;
        ParseManifestIndex = parseManifestIndex;
        FailureMetrics = failureMetrics;
    }

    internal string DataDir { get; }

    internal IManifestRetentionFailureMetrics FailureMetrics { get; }

    internal IStorageFileOperations FileOperations { get; }

    internal ILogger? Logger { get; }

    internal string ManifestFileGlob { get; }

    internal int ManifestRetention { get; }

    internal Func<string, int> ParseManifestIndex { get; }

    internal int SnapshotRetention { get; }
}
