using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A write the shutdown refused before any entry of it was scheduled reaches the client as having no leader, never as an unknown outcome.</summary>
public sealed class ReplicaCommitterRefusalTests : IsolatedStorageTestBase
{
    private const int CommitUnknownEventId = 4006;
    private const string OwnedGroup = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan LogShutdownBudget = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A write queued on the log gate behind a stuck holder is refused as having no leader when the registry dispose gives up on the holder:
    /// nothing was scheduled, so it is not COMMIT_OUTCOME_UNKNOWN, and the log keeps only the entry committed before.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimedOutLogRefusesWriteAsNoLeader(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var log = new EventRecordingLogger();
        using var loggerFactory = new FixedLoggerFactory(log);
        var clock = new DueTimerClock(LogShutdownBudget);
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var registry = await OpenRegistryAsync(
            new FollowerLogOptions { TimeProvider = TimeProvider.System, FaultHooks = hooks, ShutdownBudget = LogShutdownBudget, ShutdownTimeProvider = clock },
            loggerFactory,
            cancellationToken);
        var committer = CreateCommitter(registry, local, log);
        try
        {
            await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);
            var owned = GetOwnedLog(registry);
            var status = await owned.GetStatusAsync(cancellationToken);
            hooks.StallNextMetaWrite();
            var holder = owned.ObserveTermAsync(status.CurrentTerm + 1UL, CancellationToken.None);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var disposing = registry.DisposeAsync().AsTask();
            _ = await Assert.That(await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), CancellationToken.None);

            clock.Advance(LogShutdownBudget);
            await disposing.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await AssertNoLeaderAsync(write, cancellationToken);
            _ = await Assert.That(log.Count(CommitUnknownEventId)).IsEqualTo(0);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(holder.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await ReleaseStallAsync(hooks, committer, registry);
        }

        _ = await Assert.That(await ReopenedLastLogIndexAsync(cancellationToken)).IsEqualTo(1UL);
    }

    /// <summary>
    /// A write whose frames the drained ack registry of the log refuses to track is refused as having no leader, not as an unknown outcome:
    /// nothing was scheduled, and the log keeps only the entry committed before.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainedRegistryRefusesWriteAsNoLeader(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var log = new EventRecordingLogger();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), NullLoggerFactory.Instance, cancellationToken);
        await using var committer = CreateCommitter(registry, local, log);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);
        IFollowerLogDurability durable = GetOwnedLog(registry);
        _ = durable.Acks.FaultAll(new ObjectDisposedException(nameof(FollowerLog)));

        var refused = NewOperationId();
        await AssertNoLeaderAsync(committer.CommitSetAsync(refused, "cache", "k2", Entry(), cancellationToken), cancellationToken);

        // The refused operation holds no reservation: its retry is refused the same way instead of being told its outcome is unknown.
        await AssertNoLeaderAsync(committer.CommitSetAsync(refused, "cache", "k2", Entry(), cancellationToken), cancellationToken);

        _ = await Assert.That(log.Count(CommitUnknownEventId)).IsEqualTo(0);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);
    }

    /// <summary>A write that reaches a log disposed after it drained is refused as having no leader, and the log keeps only the entry committed before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DrainedLogRefusesWriteAsNoLeader(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var log = new EventRecordingLogger();
        var registry = await OpenRegistryAsync(DefaultOptions(), NullLoggerFactory.Instance, cancellationToken);
        await using var committer = CreateCommitter(registry, local, log);
        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);
        await GetOwnedLog(registry).DisposeAsync();

        await AssertNoLeaderAsync(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken), cancellationToken);

        _ = await Assert.That(log.Count(CommitUnknownEventId)).IsEqualTo(0);
        await registry.DisposeAsync();
        _ = await Assert.That(await ReopenedLastLogIndexAsync(cancellationToken)).IsEqualTo(1UL);
    }

    /// <summary>A write on a committer that is not started yet, whose log gate was disposed behind a stuck holder, is refused as having no leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnstartedWriteOnTimedOutLogIsNoLeader(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var log = new EventRecordingLogger();
        using var loggerFactory = new FixedLoggerFactory(log);
        var clock = new DueTimerClock(LogShutdownBudget);
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var registry = await OpenRegistryAsync(
            new FollowerLogOptions { TimeProvider = TimeProvider.System, FaultHooks = hooks, ShutdownBudget = LogShutdownBudget, ShutdownTimeProvider = clock },
            loggerFactory,
            cancellationToken);
        var committer = CreateCommitter(registry, local, log);
        try
        {
            var owned = GetOwnedLog(registry);
            var status = await owned.GetStatusAsync(cancellationToken);
            hooks.StallNextMetaWrite();
            var holder = owned.ObserveTermAsync(status.CurrentTerm + 1UL, CancellationToken.None);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var disposing = registry.DisposeAsync().AsTask();
            _ = await Assert.That(await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
            clock.Advance(LogShutdownBudget);
            await disposing.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await AssertNoLeaderAsync(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken), cancellationToken);

            _ = await Assert.That(log.Count(CommitUnknownEventId)).IsEqualTo(0);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(holder.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        }
        finally
        {
            await ReleaseStallAsync(hooks, committer, registry);
        }

        _ = await Assert.That(await ReopenedLastLogIndexAsync(cancellationToken)).IsEqualTo(0UL);
    }

    /// <summary>A write on a committer that is not started yet, whose log was closed cleanly, is refused as having no leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnstartedWriteOnClosedLogIsNoLeader(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var log = new EventRecordingLogger();
        var registry = await OpenRegistryAsync(DefaultOptions(), NullLoggerFactory.Instance, cancellationToken);
        await using var committer = CreateCommitter(registry, local, log);
        await GetOwnedLog(registry).DisposeAsync();

        await AssertNoLeaderAsync(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken), cancellationToken);

        _ = await Assert.That(log.Count(CommitUnknownEventId)).IsEqualTo(0);
        await registry.DisposeAsync();
        _ = await Assert.That(await ReopenedLastLogIndexAsync(cancellationToken)).IsEqualTo(0UL);
    }

    /// <summary>A retained unresolved entry of the operation answers COMMIT_OUTCOME_UNKNOWN even when the committer was disposed meanwhile.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetainedEntryWinsOnDisposedCommitter(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        await using var registry = await OpenRegistryAsync(DefaultOptions(), NullLoggerFactory.Instance, cancellationToken);
        var committer = CreateCommitter(registry, local, NullLogger<ReplicaGroupCommitter>.Instance);
        var operationId = NewOperationId();
        var first = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, "cache", "k1", Entry(), cancellationToken));
        await committer.DisposeAsync();

        var retry = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, "cache", "k1", Entry(), cancellationToken));

        _ = await Assert.That(first.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(retry.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
    }

    /// <summary>A retained unresolved entry of the operation answers COMMIT_OUTCOME_UNKNOWN when the start of a new committer fails with a disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetainedEntryWinsOnStartRefusal(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var log = new EventRecordingLogger();
        using var loggerFactory = new FixedLoggerFactory(log);
        var clock = new DueTimerClock(LogShutdownBudget);
        var registry = await OpenRegistryAsync(
            new FollowerLogOptions { TimeProvider = TimeProvider.System, FaultHooks = hooks, ShutdownBudget = LogShutdownBudget, ShutdownTimeProvider = clock },
            loggerFactory,
            cancellationToken);
        var operationId = NewOperationId();
        var original = CreateCommitter(registry, new ScriptedApplyCache(ApplyMode.Fail), log);
        _ = await NodeAsyncAssert.ThrowsAsync<SquirixException>(original.CommitSetAsync(operationId, "cache", "k1", Entry(), cancellationToken));
        await original.DisposeAsync();
        var committer = CreateCommitter(registry, new ScriptedApplyCache(ApplyMode.Fail), log);
        try
        {
            var owned = GetOwnedLog(registry);
            var status = await owned.GetStatusAsync(cancellationToken);
            hooks.StallNextMetaWrite();
            _ = owned.ObserveTermAsync(status.CurrentTerm + 1UL, CancellationToken.None);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var disposing = registry.DisposeAsync().AsTask();
            _ = await Assert.That(await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
            clock.Advance(LogShutdownBudget);
            await disposing.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var retry = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, "cache", "k1", Entry(), cancellationToken));

            _ = await Assert.That(retry.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        }
        finally
        {
            await ReleaseStallAsync(hooks, committer, registry);
        }
    }

    /// <summary>A write whose prepare meets a disposed local cache is refused as having no leader, before anything is appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PrepareOnDisposedCacheIsNoLeader(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), NullLoggerFactory.Instance, cancellationToken);
        await using var committer = CreateCommitter(registry, local, NullLogger<ReplicaGroupCommitter>.Instance);

        var write = committer.CommitAsync(
            ("cache", NewOperationId()),
            0,
            static _ => [],
            static (_, _, _, _) => ValueTask.FromException<PreparedReplicaMutation>(new ObjectDisposedException("cache")),
            static _ => ValueTask.FromResult(0),
            cancellationToken);

        await AssertNoLeaderAsync(write, cancellationToken);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(0UL);
    }

    /// <summary>A disposal raised where the frames are scheduled is not a refusal before scheduling: the write keeps its plain disposal instead of being answered as no leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HookDisposalIsNotAnswerNoLeader(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFramesScheduling().Callback(static () => throw new ObjectDisposedException("hook"));
        _ = faults.Setups.OnMetaPublishing();
        var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System, FaultHooks = faults.Instance() };
        await using var registry = await OpenRegistryAsync(options, NullLoggerFactory.Instance, cancellationToken);
        await using var committer = CreateCommitter(registry, local, NullLogger<ReplicaGroupCommitter>.Instance);

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));
    }

    private static async Task AssertNoLeaderAsync(Task write, CancellationToken cancellationToken)
    {
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(refused.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
    }

    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, ScriptedApplyCache local, ILogger<ReplicaGroupCommitter> log) =>
        new(registry, new TwoNodeLocator(), new AcceptingGateway(), local, (OwnedGroup, OwnedGroup), new ReplicaTopologyStamp(Fingerprint, 1), log)
        {
            Clock = TimeProvider.System,
            ShutdownTimeProvider = TimeProvider.System,
            BudgetTimeProvider = TimeProvider.System,
            Recovery = RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, OwnedGroup, OwnedGroup),
            ShutdownBudget = LogShutdownBudget,
        };

    private static FollowerLogOptions DefaultOptions() => new() { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };

    private static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    private static FollowerLog GetOwnedLog(ReplicaGroupRegistry registry) =>
        registry.TryGetLog(OwnedGroup, out var log) && log is FollowerLog followerLog ? followerLog : throw new InvalidOperationException("The owned group log is not open.");

    private static async Task<ulong> LastLogIndexAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var status = await GetOwnedLog(registry).GetStatusAsync(cancellationToken);
        return status.LastLogIndex;
    }

    private static string NewOperationId() => Guid.NewGuid().ToString("N");

    private static bool OpensForWrite(string path)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    /// <summary>Releases the stall, disposes the committer and the registry, and closes the log handle a timed-out dispose leaked, as the process exit would.</summary>
    /// <param name="hooks">The stall hooks of the registry log.</param>
    /// <param name="committer">The committer to dispose.</param>
    /// <param name="registry">The registry to dispose.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <remarks>
    /// <see cref="StallableFollowerLogFaultHooks.Exited" /> precedes the released metadata write, and a handle disposed while that write is in flight
    /// closes only when the write returns, so the metadata file is reopened only once it opens for writing again.
    /// </remarks>
    private async Task ReleaseStallAsync(StallableFollowerLogFaultHooks hooks, ReplicaGroupCommitter committer, ReplicaGroupRegistry registry)
    {
        hooks.Release();
        if (hooks.Entered.IsCompleted)
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, CancellationToken.None);

        await committer.DisposeAsync();
        await registry.DisposeAsync();
        var log = GetOwnedLog(registry);
        IFollowerLogDurability durable = log;
        durable.Durability.Dispose();
        log.Metadata.Dispose();
        var metadataPath = GroupStoragePaths.GetMetadataPath(Dir, OwnedGroup);
        _ = SpinWait.SpinUntil(() => OpensForWrite(metadataPath), StallTimeout);
    }

    private async Task<ulong> ReopenedLastLogIndexAsync(CancellationToken cancellationToken)
    {
        await using var reopened = new FollowerLog(Dir, OwnedGroup, GroupComposition.Create(OwnedGroup), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        return status.LastLogIndex;
    }

    private async Task<ReplicaGroupRegistry> OpenRegistryAsync(FollowerLogOptions options, ILoggerFactory loggerFactory, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(Dir, [OwnedGroup], cancellationToken);
        var registry = new ReplicaGroupRegistry(Dir, [OwnedGroup], 2, Fingerprint, 1, loggerFactory, options) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }
}
