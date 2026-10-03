using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An idempotent RPC on an RF=3 group owner: the group log, not the cache journal, is the durable source of the write.</summary>
[Immutable]
public sealed class ReplicatedIdempotencyTests : DisposableServerUnitTestBase
{
    private const string Fingerprint = "try-add-entry-async|default|k|abc123";
    private const string Key = "k";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// The group committed and applied the write, then the journal shut down before the outcome frame: the caller gets the unknown
    /// outcome, never the raw journal failure, and a retry reaches the committer instead of finding a started record.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShutdownAfterCommitIsUnknown(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-replicated-idempotency-unknown");
        using var groupDir = new TempDirectory("squirix-replicated-idempotency-unknown-group");
        var persistence = CreatePersistence(scenario.DataDir);
        var store = CreateStore();

        await using var registry = await ReplicaOwnerTestKit.OpenRegistryAsync(groupDir, cancellationToken);
        await using var journal = await CreateJournalAsync(scenario, persistence, cancellationToken);
        await using var committer = ReplicaOwnerTestKit.CreateCommitter(registry, new ReplicaOwnerTestKit.ScriptedGateway(), new JournalingCache(journal));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            coordinator.ExecuteAsync(
                OperationId,
                Fingerprint,
                (Committer: committer, Journal: journal),
                static async (state, ct) =>
                {
                    var added = await state.Committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), ct).ConfigureAwait(false);
                    await state.Journal.DisposeAsync().ConfigureAwait(false);
                    return new TryAddAsyncResponse { Added = added };
                },
                cancellationToken));

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(failure.Status.Detail)).IsTrue();
        _ = await Assert.That(store.ReserveIntent(OperationId, Fingerprint, null, out _)).IsEqualTo(IdempotencyReserveResult.Acquired);
    }

    /// <summary>
    /// After the owner restarted before the outcome frame, a retry with the same operation id reaches the committer, which replays the
    /// group outcome: nothing is appended to the group log again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRetryReplaysGroupOutcome(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-replicated-idempotency-restart");
        using var groupDir = new TempDirectory("squirix-replicated-idempotency-restart-group");
        var persistence = CreatePersistence(scenario.DataDir);

        await using (var registry = await ReplicaOwnerTestKit.OpenRegistryAsync(groupDir, cancellationToken))
        {
            await using var journal = await CreateJournalAsync(scenario, persistence, cancellationToken);
            await using var committer = ReplicaOwnerTestKit.CreateCommitter(registry, new ReplicaOwnerTestKit.ScriptedGateway(), new JournalingCache(journal));
            var coordinator = new RpcMutationIdempotencyCoordinator(CreateStore(), journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
            _ = await NodeAsyncAssert.ThrowsAsync<RpcException>(
                coordinator.ExecuteAsync(
                    OperationId,
                    Fingerprint,
                    (Committer: committer, Journal: journal),
                    static async (state, ct) =>
                    {
                        var added = await state.Committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), ct).ConfigureAwait(false);
                        await state.Journal.DisposeAsync().ConfigureAwait(false);
                        return new TryAddAsyncResponse { Added = added };
                    },
                    cancellationToken));
        }

        var restartedStore = CreateStore();
        await RunRecoveryAsync(scenario, persistence, restartedStore, cancellationToken);
        await using var restartedRegistry = await ReplicaOwnerTestKit.OpenRegistryAsync(groupDir, cancellationToken);
        var before = await StatusAsync(restartedRegistry, cancellationToken);
        await using var restartedJournal = await CreateJournalAsync(scenario, persistence, cancellationToken);
        await using var restartedCommitter = ReplicaOwnerTestKit.CreateCommitter(restartedRegistry, new ReplicaOwnerTestKit.ScriptedGateway(), new JournalingCache(restartedJournal));
        var restartedCoordinator = new RpcMutationIdempotencyCoordinator(restartedStore, restartedJournal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);

        var retried = await restartedCoordinator.ExecuteAsync(
            OperationId,
            Fingerprint,
            restartedCommitter,
            static async (committer, ct) =>
                new TryAddAsyncResponse { Added = await committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), ct).ConfigureAwait(false) },
            cancellationToken);
        var after = await StatusAsync(restartedRegistry, cancellationToken);

        _ = await Assert.That(retried.Added).IsTrue();
        _ = await Assert.That(after.LastLogIndex).IsEqualTo(before.LastLogIndex);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static PersistenceOptions CreatePersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };

    private static async Task<FollowerLogStatus> StatusAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        _ = registry.TryGetLog("n1", out var log);
        return await log!.GetStatusAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<IJournalCoordinator> CreateJournalAsync(RecoveryScenarioBuilder scenario, PersistenceOptions persistence, CancellationToken cancellationToken) =>
        JournalCoordinatorFactory.Create(
            persistence,
            await scenario.Ledger.ReadCurrentOrDefaultAsync(cancellationToken),
            scenario.Ledger,
            new AsyncManualResetEvent(true),
            NullLoggerFactory.Instance,
            TimeProvider.System,
            out _);

    private static Task RunRecoveryAsync(
        RecoveryScenarioBuilder scenario,
        PersistenceOptions persistence,
        RpcMutationIdempotencyStore idempotencyStore,
        CancellationToken cancellationToken)
    {
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                scenario.Ledger,
                scenario.Cache,
                new AsyncManualResetEvent(true),
                idempotencyStore,
                StoreFactory.CreateReader(),
                TimeProvider.System));
        return recovery.StartAsync(cancellationToken);
    }

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));

    /// <summary>Local cache double that journals every replicated write through the real journal coordinator, as the local write chain does.</summary>
    [ThreadSafe]
    private sealed class JournalingCache : ILogicalNamespacedCache<object?>
    {
        private readonly IJournalCoordinator _journal;

        internal JournalingCache(IJournalCoordinator journal)
        {
            _journal = journal;
        }

        public ValueTask<NodeCacheEntry<object?>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult<NodeCacheEntry<object?>?>(null);

        public ValueTask<NodeCacheValueResult<object?>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new NodeCacheValueResult<object?>(false, null));

        public ValueTask<CacheRemoveResult<object?>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new CacheRemoveResult<object?>(false, null));

        public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken) =>
            _journal.AppendPutUnderGateAsync(new CacheKey(cacheName, key), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);

        public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ValueTask.FromResult(false);

        public async ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry, CancellationToken cancellationToken)
        {
            await _journal.AppendPutUnderGateAsync(new CacheKey(cacheName, key), JournalEntryPayloadKit.EncodePut("v"), cancellationToken).ConfigureAwait(false);
            return true;
        }

        public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, object? value, CancellationToken cancellationToken) => ValueTask.FromResult(false);
    }
}
