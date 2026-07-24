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
    {
        ArgumentNullException.ThrowIfNull(settings);
        ArgumentNullException.ThrowIfNull(failureMetrics);
        DataDir = settings.DataDir;
        ManifestRetention = settings.ManifestRetention;
        SnapshotRetention = settings.SnapshotRetention;
        FileOperations = fileOperations ?? new FileOperations();
        Logger = logger;
        ManifestFileGlob = settings.ManifestFileGlob;
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
