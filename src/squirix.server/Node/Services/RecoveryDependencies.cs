using System;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
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
        ISnapshotReader snapshotReader,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(persistence);
        ArgumentNullException.ThrowIfNull(manifestStore);
        ArgumentNullException.ThrowIfNull(localCache);
        ArgumentNullException.ThrowIfNull(asyncManualResetEvent);
        ArgumentNullException.ThrowIfNull(idempotency);
        ArgumentNullException.ThrowIfNull(snapshotReader);
        ArgumentNullException.ThrowIfNull(timeProvider);
        Persistence = persistence;
        Ledger = manifestStore;
        LocalCache = localCache;
        AsyncManualResetEvent = asyncManualResetEvent;
        Idempotency = idempotency;
        SnapshotReader = snapshotReader;
        TimeProvider = timeProvider;
    }

    internal AsyncManualResetEvent AsyncManualResetEvent { get; }

    /// <summary>Gets who decides expiry; under <see cref="CacheExpiryAuthority.CommittedRecords" /> replay keeps entries past their deadline.</summary>
    internal CacheExpiryAuthority Expiry { get; init; } = CacheExpiryAuthority.LocalClock;

    internal RpcMutationIdempotencyStore Idempotency { get; }

    internal Ledger Ledger { get; }

    internal ILocalCacheRecovery<T> LocalCache { get; }

    internal PersistenceOptions Persistence { get; }

    internal ISnapshotReader SnapshotReader { get; }

    /// <summary>Gets the server clock replay decides expiry with, the same clock the cache judges liveness with.</summary>
    internal TimeProvider TimeProvider { get; }
}
