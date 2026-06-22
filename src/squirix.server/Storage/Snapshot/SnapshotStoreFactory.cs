using System;
using JsonSnapshotReader = Squirix.Server.Storage.Snapshot.Json.SnapshotReader;
using JsonSnapshotWriter = Squirix.Server.Storage.Snapshot.Json.SnapshotWriter;

namespace Squirix.Server.Storage.Snapshot;

internal static class SnapshotStoreFactory
{
    public static ISnapshotWriter CreateWriter(PersistenceOptions options, IStorageFileOperations? fileOperations = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var fileOps = fileOperations ?? new StorageFileOperations();
        return options.SnapshotBackend switch
        {
            SnapshotBackend.Json => new JsonSnapshotWriter(options.DataDir, fileOps),
            SnapshotBackend.Binary => new Binary.SnapshotWriter(options.DataDir, fileOps),
            _ => throw new ArgumentOutOfRangeException(nameof(options), options.SnapshotBackend, "Unsupported snapshot backend."),
        };
    }

    public static ISnapshotReader CreateReader(PersistenceOptions options) =>
        options.SnapshotBackend switch
        {
            SnapshotBackend.Json => new JsonSnapshotReader(),
            SnapshotBackend.Binary => new Binary.SnapshotReader(),
            _ => throw new ArgumentOutOfRangeException(nameof(options), options.SnapshotBackend, "Unsupported snapshot backend."),
        };
}
