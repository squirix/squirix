using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The group log is the durable source of a replicated mutation.</summary>
[Immutable]
public sealed class ReplicatedIdempotencyTests
{
    private const string Key = "k";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

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
        var added = await committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), cancellationToken);
        await journal.DisposeAsync();
        var status = await StatusAsync(registry, cancellationToken);

        _ = await Assert.That(added).IsTrue();
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
        var added = await committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), cancellationToken);
        await journal.DisposeAsync();

        _ = await Assert.That(added).IsTrue();
    }

    /// <summary>
    /// After the owner restarted, a retry with the same operation id reaches the committer, which replays the group outcome: nothing is
    /// appended to the group log again.
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
            var first = await committer.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), cancellationToken);
            _ = await Assert.That(first).IsTrue();
        }

        await RunRecoveryAsync(scenario, persistence, cancellationToken);
        await using var restartedRegistry = await ReplicaOwnerTestKit.OpenRegistryAsync(groupDir, cancellationToken);
        var before = await StatusAsync(restartedRegistry, cancellationToken);
        await using var restartedJournal = await CreateJournalAsync(scenario, persistence, cancellationToken);
        await using var restartedCommitter = ReplicaOwnerTestKit.CreateCommitter(restartedRegistry, new ReplicaOwnerTestKit.ScriptedGateway(), new JournalingCache(restartedJournal), NullLogger<ReplicaGroupCommitter>.Instance);

        var retried = await restartedCommitter.CommitTryAddAsync(OperationId, "default", Key, ReplicaOwnerTestKit.Entry(Key), cancellationToken);
        var after = await StatusAsync(restartedRegistry, cancellationToken);

        _ = await Assert.That(retried).IsTrue();
        _ = await Assert.That(after.LastLogIndex).IsEqualTo(before.LastLogIndex);
    }

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
                StoreFactory.CreateReader(),
                TimeProvider.System));
        return recovery.StartAsync(cancellationToken);
    }
}
