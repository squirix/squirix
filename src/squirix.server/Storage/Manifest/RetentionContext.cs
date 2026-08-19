using System;
using Microsoft.Extensions.Logging;
using Squirix.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Inputs for shared manifest retention cleanup.</summary>
[Immutable]
internal sealed record RetentionContext
{
    internal RetentionContext(
        RetentionSettings settings,
        IStorageFileOperations? fileOperations,
        ILogger? logger,
        Func<string, int> parseManifestIndex,
        IManifestRetentionFailureMetrics? failureMetrics = null)
    {
        ArgumentNullException.ThrowIfNull(settings);
        DataDir = settings.DataDir;
        ManifestRetention = settings.ManifestRetention;
        SnapshotRetention = settings.SnapshotRetention;
        FileOperations = fileOperations ?? new FileOperations();
        Logger = logger;
        ManifestFileGlob = settings.ManifestFileGlob;
        ParseManifestIndex = parseManifestIndex;
        FailureMetrics = failureMetrics ?? NoOpManifestRetentionFailureMetrics.Instance;
    }

    internal string DataDir { get; }

    internal IManifestRetentionFailureMetrics FailureMetrics { get; }

    internal IStorageFileOperations FileOperations { get; }

    internal ILogger? Logger { get; }

    internal string ManifestFileGlob { get; }

    internal int ManifestRetention { get; }

    internal Func<string, int> ParseManifestIndex { get; }

    internal int SnapshotRetention { get; }

    [Immutable]
    private sealed class NoOpManifestRetentionFailureMetrics : IManifestRetentionFailureMetrics
    {
        internal static NoOpManifestRetentionFailureMetrics Instance { get; } = new();

        public void RecordDeleteFailure(string artifactKind, string outcome)
        {
            _ = artifactKind;
            _ = outcome;
        }
    }
}
