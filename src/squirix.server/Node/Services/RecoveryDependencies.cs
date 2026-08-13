using System;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

internal sealed class RecoveryDependencies<T>
{
    internal RecoveryDependencies(
        PersistenceOptions persistence,
        Ledger manifestStore,
        ILocalCacheRecovery<T> localCache,
        JournalStartupGate journalStartupGate,
        RpcMutationIdempotencyStore idempotency,
        ISnapshotReader snapshotReader)
    {
        Persistence = persistence ?? throw new ArgumentNullException(nameof(persistence));
        Ledger = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        LocalCache = localCache ?? throw new ArgumentNullException(nameof(localCache));
        JournalStartupGate = journalStartupGate ?? throw new ArgumentNullException(nameof(journalStartupGate));
        Idempotency = idempotency ?? throw new ArgumentNullException(nameof(idempotency));
        SnapshotReader = snapshotReader ?? throw new ArgumentNullException(nameof(snapshotReader));
    }

    internal RpcMutationIdempotencyStore Idempotency { get; }

    internal JournalStartupGate JournalStartupGate { get; }

    internal Ledger Ledger { get; }

    internal ILocalCacheRecovery<T> LocalCache { get; }

    internal PersistenceOptions Persistence { get; }

    internal ISnapshotReader SnapshotReader { get; }
}
