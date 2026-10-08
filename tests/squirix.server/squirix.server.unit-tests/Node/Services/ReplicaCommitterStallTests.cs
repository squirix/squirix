using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// An RF=2 group owner whose follower stalls before the majority, or whose memory apply fails or stalls after the majority acknowledged
/// the write.
/// </summary>
public sealed class ReplicaCommitterStallTests : IsolatedStorageTestBase
{
    private const int CommitUnknownEventId = 4006;
    private const string IdleGroup = "g2";
    private const int LeakedOnShutdownEventId = 4005;
    private const int LogLeakedOnShutdownEventId = 4009;
    private const string OwnedGroup = "n1";
    private const string StalledGroup = "g1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan LogShutdownBudget = TimeSpan.FromMilliseconds(200);

    /// <summary>Well below the 5 s default, yet long enough that preparing the write (log append, fsync) cannot exhaust it on a loaded machine.</summary>
    private static readonly TimeSpan ShortCommitBudget = TimeSpan.FromSeconds(1);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A write whose outcome is unknown after its local append reaches the pipeline as the stable commit-unknown contract (gRPC
    /// Unavailable with COMMIT_OUTCOME_UNKNOWN), and the domain error mapping keeps it that way instead of turning it into a precondition failure.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicatedUnknownMapsToStableContract(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        var log = new LeakRecordingLogger();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local, log);
        var cache = new DomainErrorMappingCacheDecorator<object?>(new ReplicatedCache(local, LedGroupsTestKit.LeadOwn(committer)));

        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(cache.SetEntryAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));
        var transport = error.ToRpcException();

        // The remote owner reports the stable commit-unknown contract over gRPC.
        var remoteOwner = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = remoteOwner.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                       .Callback(static (_, _, _, _, _) => ValueTask.FromException(ServerOpContract.CommitOutcomeUnknown().ToRpcException()));
        var forwarded = new DomainErrorMappingCacheDecorator<object?>(remoteOwner.Instance());
        var remote = await NodeAsyncAssert.ThrowsAsync<RpcException>(forwarded.SetEntryAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(transport.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(transport.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(remote.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(remote.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(log.UnknownLevel).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(log.UnknownCause?.InnerException?.Message).IsEqualTo("Injected memory apply failure after the majority.");
    }

    /// <summary>
    /// A write whose follower never answers ends with COMMIT_OUTCOME_UNKNOWN once a shortened commit budget expires on the committer's clock, with
    /// its entry kept in the local log.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShortCommitBudgetYieldsUnknown(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        var log = new LeakRecordingLogger();
        var gateway = new StalledGateway();
        var clock = new DueTimerClock(ShortCommitBudget);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var defaults = CreateCommitter(registry, local, log);
        await using var committer = new ReplicaGroupCommitter(registry, new TwoNodeLocator(), gateway, local, (OwnedGroup, OwnedGroup), new ReplicaTopologyStamp(Fingerprint, 1), log)
        {
            Recovery = RecoveryLifecycle.Recovered(),
            CommitBudget = ShortCommitBudget,
            BudgetTimeProvider = clock,
            ShutdownBudget = LogShutdownBudget,
        };
        try
        {
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);

            // The budget runs on the fake clock, so it cannot expire before the follower call; once that call is stalled, the budget is armed
            // and moving the clock past it ends the commit.
            await gateway.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            clock.Advance(ShortCommitBudget);
            var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

            _ = await Assert.That(defaults.CommitBudget).IsEqualTo(TimeSpan.FromSeconds(5));
            _ = await Assert.That(committer.CommitBudget).IsEqualTo(ShortCommitBudget);
            _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
            _ = await Assert.That(log.UnknownLevel).IsEqualTo(LogLevel.Warning);
            _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);
            _ = await Assert.That(local.Applied.IsEmpty).IsTrue();
        }
        finally
        {
            gateway.Release();
        }
    }

    /// <summary>
    /// Disposing the committer while a write past its majority is stuck in the memory apply returns within the shutdown budget, logs the
    /// leak, refuses new writes, and leaves the in-flight write to complete once the apply recovers.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitterDisposeBoundedUnderStall(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Stall);
        var log = new LeakRecordingLogger();
        await using var registry = await OpenRegistryAsync(cancellationToken);
        var committer = CreateCommitter(registry, local, log);
        try
        {
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);
            await local.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await committer.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var refused = committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(refused);
            _ = await Assert.That(log.LeakCount).IsEqualTo(1);

            local.ReleaseApply();
            await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            local.ReleaseApply();
            await committer.DisposeAsync();
        }
    }

    /// <summary>
    /// A write stuck in its commit-index advance after the majority, left behind by the committer's shutdown leak, is released by the
    /// registry dispose within the log budget: the caller gets COMMIT_OUTCOME_UNKNOWN caused by the dispose, and the log reports its leak.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LogDisposeReleasesLeakedCommit(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var log = new LeakRecordingLogger();
        using var loggerFactory = new FixedLoggerFactory(log);
        var registry = await OpenRegistryAsync([OwnedGroup], StallOptions(hooks), loggerFactory, cancellationToken);
        var committer = CreateCommitter(registry, new ScriptedApplyCache(ApplyMode.Fail), log, new AcceptingGateway(hooks.StallNextMetaWrite));
        try
        {
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await committer.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await registry.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
            _ = await Assert.That(log.UnknownCause?.InnerException).IsTypeOf<ObjectDisposedException>();
            _ = await Assert.That(log.Count(LeakedOnShutdownEventId)).IsEqualTo(1);
            _ = await Assert.That(log.Count(LogLeakedOnShutdownEventId)).IsEqualTo(1);
            _ = await Assert.That(log.FaultedWaiters).IsEqualTo(1);
        }
        finally
        {
            await ReleaseStallAsync(hooks, committer, registry, OwnedGroup);
        }
    }

    /// <summary>
    /// A write stuck in its local frame flush, before the append counts, is released by the registry dispose after the committer's
    /// shutdown leak with the raw <see cref="ObjectDisposedException" />, not COMMIT_OUTCOME_UNKNOWN.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LogDisposeReleasesPreAppendStall(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var log = new LeakRecordingLogger();
        using var loggerFactory = new FixedLoggerFactory(log);
        var registry = await OpenRegistryAsync([OwnedGroup], StallOptions(hooks), loggerFactory, cancellationToken);
        var committer = CreateCommitter(registry, new ScriptedApplyCache(ApplyMode.Fail), log);
        try
        {
            hooks.StallNextFrameWrite();
            var write = committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            await committer.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            await registry.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(log.Count(CommitUnknownEventId)).IsEqualTo(0);
            _ = await Assert.That(log.Count(LogLeakedOnShutdownEventId)).IsEqualTo(1);
            _ = await Assert.That(log.FaultedWaiters).IsEqualTo(1);
        }
        finally
        {
            await ReleaseStallAsync(hooks, committer, registry, OwnedGroup);
        }
    }

    /// <summary>
    /// With one group log stuck in a flush, the registry dispose closes the other log without waiting for the stuck one's budget, which
    /// runs on a fake clock that moves only after the idle log closed, and returns once that single budget expired; the other group's directory can be reopened.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The stalled group log is not open.</exception>
    [Test]
    public async Task RegistryDisposeBoundedWithStalledLog(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        var log = new LeakRecordingLogger();
        using var loggerFactory = new FixedLoggerFactory(log);
        var clock = new DueTimerClock(LogShutdownBudget);
        var registry = await OpenRegistryAsync(
            [StalledGroup, IdleGroup],
            new FollowerLogOptions { FaultHooks = hooks, ShutdownBudget = LogShutdownBudget, ShutdownTimeProvider = clock },
            loggerFactory,
            cancellationToken);
        var idleLogPath = FollowerLogPaths.Create(Dir, IdleGroup).LogPath;
        try
        {
            if (!registry.TryGetLog(StalledGroup, out var stalled))
                throw new InvalidOperationException("The stalled group log is not open.");

            hooks.StallNextFrameWrite();
            var first = new FollowerLogAppendRequest("leader-1", 1UL, 0UL, 0UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("a"))));
            var append = stalled.AppendAsync(first, cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(IsReleased(idleLogPath)).IsFalse();

            // The stuck log's budget cannot expire until the clock moves, so seeing the idle log closed without the leak event proves the
            // idle log was not queued behind the stuck one.
            var idleClosedWhileStuck = false;
            var disposing = registry.DisposeAsync().AsTask();
            var idleClosed = SpinWait.SpinUntil(
                () =>
                {
                    if (!IsReleased(idleLogPath))
                        return false;

                    idleClosedWhileStuck = log.Count(LogLeakedOnShutdownEventId) == 0;
                    return true;
                },
                StallTimeout);
            _ = await Assert.That(idleClosed).IsTrue();
            _ = await Assert.That(idleClosedWhileStuck).IsTrue();
            _ = await Assert.That(disposing.IsCompleted).IsFalse();

            // Both group logs arm a budget timer on this clock when the registry dispose starts: wait for both before moving it.
            _ = await Assert.That(await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
            _ = await Assert.That(await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
            clock.Advance(LogShutdownBudget);
            await disposing.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(append.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(log.Count(LogLeakedOnShutdownEventId)).IsEqualTo(1);
        }
        finally
        {
            await ReleaseStallAsync(hooks, null, registry, StalledGroup);
        }

        await using var reopened = new FollowerLog(Dir, IdleGroup, GroupComposition.Create(IdleGroup), NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
    }

    /// <summary>
    /// While a committed entry is still unapplied, a new write is refused before its local append with a definite, retryable error
    /// (not COMMIT_OUTCOME_UNKNOWN), because its outcome would be prepared from memory that misses the entry; once the apply recovers,
    /// the retried write applies after the entry, in log order.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingApplyRefusesWriteDefinitely(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local);
        var first = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));
        _ = await Assert.That(first.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);

        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken));
        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(refused.ToRpcException().StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);

        local.Recover();
        await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken);
        await SequenceAssert.EqualAsync(["k1", "k2"], local.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// A committer resync (the group lost and regained its write majority) applies the old coordinator's committed-but-unapplied entry
    /// before replacing the coordinator, instead of dropping it together with the coordinator.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResyncDrivesCommittedPendingApply(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local);
        await FailFirstWriteThenDropMajorityAsync(committer, registry, cancellationToken);

        local.Recover();
        await committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry(), cancellationToken);

        await SequenceAssert.EqualAsync(["k1", "k3"], local.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// A committer resync whose committed entry still cannot be applied refuses the write definitely and keeps the old coordinator with
    /// the entry, so the next resync after the apply recovers applies it before any newer write.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResyncKeepsPendingApply(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        await using var registry = await OpenRegistryAsync(cancellationToken);
        await using var committer = CreateCommitter(registry, local);
        await FailFirstWriteThenDropMajorityAsync(committer, registry, cancellationToken);

        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry(), cancellationToken));
        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);

        local.Recover();
        await committer.CommitSetAsync(NewOperationId(), "cache", "k4", Entry(), cancellationToken);
        await SequenceAssert.EqualAsync(["k1", "k4"], local.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>
    /// A new operation refused because the group idempotency state is full gets a retryable refusal before anything is appended, and the
    /// committer keeps running: a retry of a recorded operation still replays its outcome.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FullIdempotencyRefusesRetryably(CancellationToken cancellationToken)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        await using var registry = await OpenRegistryAsync([OwnedGroup], new FollowerLogOptions { IdempotencyCapacity = 1 }, NullLoggerFactory.Instance, cancellationToken);
        await using var committer = CreateCommitter(registry, local);
        var recorded = NewOperationId();
        await committer.CommitSetAsync(recorded, "cache", "k1", Entry(), cancellationToken);

        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken));
        var logIndex = await LastLogIndexAsync(registry, cancellationToken);
        await committer.CommitSetAsync(recorded, "cache", "k1", Entry(), cancellationToken);

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(logIndex).IsEqualTo(1UL);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(1UL);
    }

    /// <summary>Commits a write whose apply fails after its majority, then demotes the follower so the next write drops the started state.</summary>
    /// <param name="committer">The committer under test.</param>
    /// <param name="registry">The registry owning the group log and eligibility.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task FailFirstWriteThenDropMajorityAsync(ReplicaGroupCommitter committer, ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry(), cancellationToken));
        _ = await Assert.That(unknown.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);

        // Invalid progress demotes a ready follower to catching up: the next write has no verified majority and drops the started
        // state, so the write after it resyncs (a new coordinator whose start re-verifies the follower).
        _ = registry.EligibilityFor("n1").TryMarkCatchingUp(1, default);
        var refusal = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry(), cancellationToken));
        _ = await Assert.That(refusal.StatusCode).IsEqualTo(StatusCode.Unavailable);
    }

    private static async Task<ulong> LastLogIndexAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        return status.LastLogIndex;
    }

    private static ReplicaGroupCommitter CreateCommitter(
        ReplicaGroupRegistry registry,
        ILogicalNamespacedCache<object?> local,
        ILogger<ReplicaGroupCommitter>? log = null,
        IReplicaRpcGateway? gateway = null) =>
        new(registry, new TwoNodeLocator(), gateway ?? new AcceptingGateway(), local, ("n1", "n1"), new ReplicaTopologyStamp(Fingerprint, 1), log ?? new LeakRecordingLogger())
        {
            Recovery = RecoveryLifecycle.Recovered(),
            ShutdownBudget = TimeSpan.FromMilliseconds(200),
        };

    private static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    /// <summary>Tells whether the log file can be opened exclusively, which holds only once its follower log closed its handle.</summary>
    /// <param name="path">The follower log file path.</param>
    /// <returns><see langword="true" /> when no follower log holds the file open.</returns>
    private static bool IsReleased(string path)
    {
        try
        {
            using var probe = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
    }

    private static string NewOperationId() => Guid.NewGuid().ToString("N");

    private static FollowerLogOptions StallOptions(StallableFollowerLogFaultHooks hooks) =>
        new() { FaultHooks = hooks, ShutdownBudget = LogShutdownBudget };

    private Task<ReplicaGroupRegistry> OpenRegistryAsync(CancellationToken cancellationToken) => OpenRegistryAsync([OwnedGroup], null, NullLoggerFactory.Instance, cancellationToken);

    /// <summary>
    /// Releases the stall, disposes the committer and the registry, and closes the log handle a timed-out dispose leaked, as the
    /// process exit would.
    /// </summary>
    /// <param name="hooks">The stall hooks of the registry logs.</param>
    /// <param name="committer">The committer to dispose, if any.</param>
    /// <param name="registry">The registry to dispose.</param>
    /// <param name="groupId">The group whose log may have been leaked.</param>
    /// <returns>An asynchronous operation.</returns>
    private async Task ReleaseStallAsync(StallableFollowerLogFaultHooks hooks, ReplicaGroupCommitter? committer, ReplicaGroupRegistry registry, string groupId)
    {
        hooks.Release();
        if (hooks.Entered.IsCompleted)
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, CancellationToken.None);

        // A released metadata write still publishes the file; it must finish before the directory is removed.
        var metadataTempPath = FollowerLogPaths.Create(Dir, groupId).MetadataTempPath;
        _ = SpinWait.SpinUntil(() => !File.Exists(metadataTempPath), StallTimeout);
        if (committer != null)
            await committer.DisposeAsync();

        await registry.DisposeAsync();
        if (registry.TryGetLog(groupId, out var log) && log is IFollowerLogDurability durable)
            durable.Durability.Dispose();
    }

    private async Task<ReplicaGroupRegistry> OpenRegistryAsync(string[] groupIds, FollowerLogOptions? options, ILoggerFactory loggerFactory, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(Dir, groupIds, 2, Fingerprint, 1, loggerFactory, options);
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

    /// <summary>Follower double whose appends stay unanswered until released, and then fail the transport.</summary>
    [ThreadSafe]
    private sealed class StalledGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Entered => _entered.Task;

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            _ = _entered.TrySetResult();
            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            throw new IOException("follower is down");
        }

        internal void Release() => _ = _released.TrySetResult();
    }

    /// <summary>
    /// Logger double counting events, capturing the committer's commit-unknown warning and the faulted waiter count of the follower-log
    /// leak event.
    /// </summary>
    [ThreadSafe]
    private sealed class LeakRecordingLogger : ILogger<ReplicaGroupCommitter>, ILogger<FollowerLog>
    {
        private readonly ConcurrentQueue<EventId> _events = new();
        private readonly ConcurrentQueue<object?> _faultedWaiters = new();
        private readonly ConcurrentQueue<(LogLevel Level, Exception? Cause)> _unknowns = new();

        internal object? FaultedWaiters => _faultedWaiters.TryPeek(out var faulted) ? faulted : null;

        internal int LeakCount => Count(LeakedOnShutdownEventId);

        internal Exception? UnknownCause => _unknowns.TryPeek(out var unknown) ? unknown.Cause : null;

        internal LogLevel? UnknownLevel => _unknowns.TryPeek(out var unknown) ? unknown.Level : null;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            _events.Enqueue(eventId);
            if (eventId.Id == LogLeakedOnShutdownEventId && state is IReadOnlyList<KeyValuePair<string, object?>> values)
            {
                foreach (var value in values)
                {
                    if (string.Equals(value.Key, "FaultedInFlightWaiters", StringComparison.Ordinal))
                        _faultedWaiters.Enqueue(value.Value);
                }
            }

            if (eventId.Id != CommitUnknownEventId)
                return;

            _unknowns.Enqueue((logLevel, exception));
        }

        internal int Count(int id)
        {
            var count = 0;
            foreach (var eventId in _events)
            {
                if (eventId.Id == id)
                    count++;
            }

            return count;
        }
    }
}
