using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Manifest.Binary;
using Squirix.Server.Storage.Manifest.Json;

namespace Squirix.Server.Storage.Manifest;

internal static class ManifestStoreFactory
{
    internal static IManifestStore Create(
        PersistenceOptions options,
        ILogger<ManifestStore>? logger = null,
        IRetentionCleanupReadinessStatus? retentionReadiness = null,
        IStorageFileOperations? fileOperations = null)
    {
        fileOperations ??= new StorageFileOperations();
        return options.ManifestBackend switch
        {
            ManifestBackend.Json => new JsonManifestStore(options, logger, retentionReadiness, fileOperations),
            ManifestBackend.Binary => new BinaryManifestStore(options, logger, retentionReadiness, fileOperations),
            _ => throw new System.ArgumentOutOfRangeException(nameof(options), options.ManifestBackend, "Unknown manifest backend."),
        };
    }
}
