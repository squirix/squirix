using System;
using Squirix.Server.Attributes;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

[Immutable]
internal sealed class RecoveryDependencies<T>
{
    internal RecoveryDependencies(
        PersistenceOptions persistence,
        Ledger manifestStore,
        ILocalCacheRecovery<T> localCache,
        AsyncManualResetEvent asyncManualResetEvent,
        RpcMutationIdempotencyStore idempotency,
        ISnapshotReader snapshotReader)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        ArgumentNullException.ThrowIfNull(manifestStore);
        ArgumentNullException.ThrowIfNull(localCache);
        ArgumentNullException.ThrowIfNull(asyncManualResetEvent);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(snapshotReader);
        Persistence = persistence;
        Ledger = manifestStore;
        LocalCache = localCache;
        AsyncManualResetEvent = asyncManualResetEvent;
        Idempotency = idempotency;
        SnapshotReader = snapshotReader;
    }

    internal AsyncManualResetEvent AsyncManualResetEvent { get; }

    internal RpcMutationIdempotencyStore Idempotency { get; }

    internal Ledger Ledger { get; }

    internal ILocalCacheRecovery<T> LocalCache { get; }

    internal PersistenceOptions Persistence { get; }

    internal ISnapshotReader SnapshotReader { get; }
}
