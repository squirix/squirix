using System;
using Squirix.Server.Storage.Snapshot.Binary;

namespace Squirix.Server.Storage.Snapshot;

internal static class StoreFactory
{
    internal static ISnapshotReader CreateReader(PersistenceOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return new SnapshotReader();
    }

    internal static ISnapshotWriter CreateWriter(PersistenceOptions options, IStorageFileOperations? fileOperations = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var fileOps = fileOperations ?? new FileOperations();
        return new SnapshotWriter(options.DataDir, fileOps);
    }
}
