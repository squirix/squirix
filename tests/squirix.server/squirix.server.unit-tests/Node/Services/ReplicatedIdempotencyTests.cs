using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An idempotent RPC on an RF=3 group owner: the group log, not the cache journal or the node-wide store, is the durable source of the write.</summary>
[Immutable]
public sealed class ReplicatedIdempotencyTests : DisposableServerUnitTestBase
{
    private const string Fingerprint = "try-add-entry-async|default|k|abc123";
    private const string Key = "k";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// The group committed and applied the write, then the journal shut down before any cache-journal frame: the group log is the durable
    /// source, so the caller gets the committed outcome and not an unknown one.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShutdownAfterCommitStaysSuccess(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-replicated-idempotency-shutdown");
        using var groupDir = new TempDirectory("squirix-replicated-idempotency-shutdown-group");
        var persistence = CreatePersistence(scenario.DataDir);

        await using var registry = await ReplicaOwnerTestKit.OpenRegistryAsync(groupDir, cancellationToken);
        await using var journal = await CreateJournalAsync(scenario, persistence, cancellationToken);
        await using var committer = ReplicaOwnerTestKit.CreateCommitter(registry, new ReplicaOwnerTestKit.ScriptedGateway(), new JournalingCache(journal), NullLogger<ReplicaGroupCommitter>.Instance);
        var coordinator = new ReplicatedMutationCoordinator(journal);

        var response = await coordinator.ExecuteAsync(
            OperationId,
            static _ => Fingerprint,
            (Committer: committer, Journal: journal),
            static async (state, ct) =>
            {
                var added = await state.Committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), ct).ConfigureAwait(false);
                await state.Journal.DisposeAsync().ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = added };
            },
            cancellationToken);
        var status = await StatusAsync(registry, cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(status.CommitIndex).IsGreaterThan(0UL);
    }

    /// <summary>
    /// A committed entry whose effect writes no cache frame is answered with its committed outcome as well: a journal shutdown after the
    /// group commit is not a failure of the write.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FramelessCommitStaysSuccess(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-replicated-idempotency-frameless");
        using var groupDir = new TempDirectory("squirix-replicated-idempotency-frameless-group");
        var persistence = CreatePersistence(scenario.DataDir);

        await using var registry = await ReplicaOwnerTestKit.OpenRegistryAsync(groupDir, cancellationToken);
        await using var journal = await CreateJournalAsync(scenario, persistence, cancellationToken);
        await using var committer = ReplicaOwnerTestKit.CreateCommitter(registry, new ReplicaOwnerTestKit.ScriptedGateway(), new ReplicaOwnerTestKit.StubCache(), NullLogger<ReplicaGroupCommitter>.Instance);
        var coordinator = new ReplicatedMutationCoordinator(journal);

        var response = await coordinator.ExecuteAsync(
            OperationId,
            static _ => Fingerprint,
            (Committer: committer, Journal: journal),
            static async (state, ct) =>
            {
                var added = await state.Committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), ct).ConfigureAwait(false);
                await state.Journal.DisposeAsync().ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = added };
            },
            cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
    }

    /// <summary>
    /// After the owner restarted, a retry with the same operation id reaches the committer, which replays the group outcome: nothing is
    /// appended to the group log again, and the node-wide store takes no part.
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
            await using var committer = ReplicaOwnerTestKit.CreateCommitter(registry, new ReplicaOwnerTestKit.ScriptedGateway(), new JournalingCache(journal), NullLogger<ReplicaGroupCommitter>.Instance);
            var first = await new ReplicatedMutationCoordinator(journal).ExecuteAsync(
                OperationId,
                static _ => Fingerprint,
                committer,
                static async (state, ct) =>
                    new TryAddAsyncResponse { Added = await state.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), ct).ConfigureAwait(false) },
                cancellationToken);
            _ = await Assert.That(first.Added).IsTrue();
        }

        var restartedStore = CreateStore();
        await RunRecoveryAsync(scenario, persistence, restartedStore, cancellationToken);
        await using var restartedRegistry = await ReplicaOwnerTestKit.OpenRegistryAsync(groupDir, cancellationToken);
        var before = await StatusAsync(restartedRegistry, cancellationToken);
        await using var restartedJournal = await CreateJournalAsync(scenario, persistence, cancellationToken);
        await using var restartedCommitter = ReplicaOwnerTestKit.CreateCommitter(restartedRegistry, new ReplicaOwnerTestKit.ScriptedGateway(), new JournalingCache(restartedJournal), NullLogger<ReplicaGroupCommitter>.Instance);

        var retried = await new ReplicatedMutationCoordinator(restartedJournal).ExecuteAsync(
            OperationId,
            static _ => Fingerprint,
            restartedCommitter,
            static async (committer, ct) =>
                new TryAddAsyncResponse { Added = await committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), ct).ConfigureAwait(false) },
            cancellationToken);
        var after = await StatusAsync(restartedRegistry, cancellationToken);

        _ = await Assert.That(retried.Added).IsTrue();
        _ = await Assert.That(after.LastLogIndex).IsEqualTo(before.LastLogIndex);
        _ = await Assert.That(restartedStore.RecordCount).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static PersistenceOptions CreatePersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16 };

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

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter), TimeProvider.System);
}
