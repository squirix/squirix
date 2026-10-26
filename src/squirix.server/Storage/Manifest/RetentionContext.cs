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
        IManifestRetentionFailureMetrics failureMetrics)
    {
        ArgumentNullException.ThrowIfNull(settings);
        DataDir = settings.DataDir;
        FileOperations = fileOperations ?? new FileOperations();
        ArgumentNullException.ThrowIfNull(logger);
        Logger = logger;
        ArgumentNullException.ThrowIfNull(failureMetrics);
        FailureMetrics = failureMetrics;
    }

    internal string DataDir { get; }

    internal IManifestRetentionFailureMetrics FailureMetrics { get; }

    internal IStorageFileOperations FileOperations { get; }

    internal ILogger Logger { get; }
}
