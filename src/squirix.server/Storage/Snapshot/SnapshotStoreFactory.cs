using System;

namespace Squirix.Server.Storage.Snapshot;

internal static class SnapshotStoreFactory
{
    public static ISnapshotWriter CreateWriter(PersistenceOptions options, IStorageFileOperations? fileOperations = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var fileOps = fileOperations ?? new StorageFileOperations();
        return new Binary.SnapshotWriter(options.DataDir, fileOps);
    }

    public static ISnapshotReader CreateReader(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new Binary.SnapshotReader();
    }
}
