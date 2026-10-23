using System;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Inputs for shared manifest retention cleanup.</summary>
[Immutable]
internal sealed record RetentionContext
{
    internal RetentionContext(
        RetentionSettings settings,
        IStorageFileOperations? fileOperations,
        ILogger logger,
        IManifestRetentionFailureMetrics? failureMetrics = null)
    {
        ArgumentNullException.ThrowIfNull(settings);
        DataDir = settings.DataDir;
        FileOperations = fileOperations ?? new FileOperations();
        ArgumentNullException.ThrowIfNull(logger);
        Logger = logger;
        FailureMetrics = failureMetrics ?? NoOpManifestRetentionFailureMetrics.Instance;
    }

    internal string DataDir { get; }

    internal IManifestRetentionFailureMetrics FailureMetrics { get; }

    internal IStorageFileOperations FileOperations { get; }

    internal ILogger Logger { get; }

    [Immutable]
    private sealed class NoOpManifestRetentionFailureMetrics : IManifestRetentionFailureMetrics
    {
        internal static NoOpManifestRetentionFailureMetrics Instance { get; } = new();

        public void RecordDeleteFailure(string artifactKind, string outcome)
        {
        }
    }
}
