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
        Persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
        Ledger = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        LocalCache = localCache ?? throw new ArgumentNullException(nameof(localCache));
        AsyncManualResetEvent = asyncManualResetEvent ?? throw new ArgumentNullException(nameof(asyncManualResetEvent));
        Idempotency = idempotency ?? throw new ArgumentNullException(nameof(idempotency));
        SnapshotReader = snapshotReader ?? throw new ArgumentNullException(nameof(snapshotReader));
    }

    internal RpcMutationIdempotencyStore Idempotency { get; }

    internal AsyncManualResetEvent AsyncManualResetEvent { get; }

    internal Ledger Ledger { get; }

    internal ILocalCacheRecovery<T> LocalCache { get; }

    internal PersistenceOptions Persistence { get; }

    internal ISnapshotReader SnapshotReader { get; }
}
