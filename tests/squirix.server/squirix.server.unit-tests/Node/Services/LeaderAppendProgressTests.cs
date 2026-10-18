using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A leader reports what its appends to the own group log did without the commit gate: failures, a log that refuses, and an append that
/// never returns. Node n1 leads group n2 from slot 2; its followers are n2 and n3.
/// </summary>
public sealed class LeaderAppendProgressTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A write whose log append throws counts as one failed append and leaves nothing in flight.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedAppendIsCounted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-failed");
        var hooks = new FailingFrameWriteHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken);
        await using var committer = await LeadAsync(registry, cancellationToken);
        var healthy = committer.AppendProgress();

        hooks.Arm();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That(healthy).IsEqualTo(new ReplicaAppendProgress(0UL, 0UL, false));
        _ = await Assert.That(committer.AppendProgress()).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, false));
    }

    /// <summary>A refusal marks the leader blocked, a plain failure keeps it so, and the next append that succeeds clears it.</summary>
    [Test]
    public async Task AppendClearsBlocked()
    {
        var appends = new ReplicaLocalAppends();

        appends.Begin();
        appends.End(ReplicaLocalAppends.Outcome.Refused);
        var refused = appends.Snapshot();
        appends.Begin();
        appends.End(ReplicaLocalAppends.Outcome.Failed);
        var failedAfter = appends.Snapshot();
        appends.Begin();
        appends.End(ReplicaLocalAppends.Outcome.Appended);

        _ = await Assert.That(refused).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, true));
        _ = await Assert.That(failedAfter).IsEqualTo(new ReplicaAppendProgress(0UL, 2UL, true));
        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(1UL, 2UL, false));
    }

    /// <summary>
    /// A log that latched its failure refuses the append as not ready, which blocks the leader; the next write cannot even start, appends
    /// nothing, and the leader stays blocked.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    [Test]
    public async Task RefusedLogMarksBlocked(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-refused");
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken);
        await using var committer = await LeadAsync(registry, cancellationToken);
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        hooks.StallNextMetaWrite();
        var latching = log.ObserveTermAsync(3UL, cancellationToken);
        await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        hooks.ReleaseWithFailure(new IOException("Injected metadata write failure."));
        await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(latching);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var refused = committer.AppendProgress();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken));

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That(refused).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, true));
        _ = await Assert.That(committer.AppendProgress().Blocked).IsTrue();
    }

    /// <summary>
    /// An append stuck in its flush is reported as in flight while it holds the commit gate, and the progress is read at once; once the
    /// flush returns, the append is counted and the leader is not blocked.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StuckAppendIsInFlight(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-stuck");
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken);
        await using var committer = await LeadAsync(registry, cancellationToken);
        try
        {
            hooks.StallNextFrameWrite();
            var write = committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
            await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var stuck = committer.AppendProgress();

            _ = await Assert.That(stuck).IsEqualTo(new ReplicaAppendProgress(0UL, 0UL, true));
            _ = await Assert.That(write.IsCompleted).IsFalse();
            hooks.Release();
            await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(committer.AppendProgress()).IsEqualTo(new ReplicaAppendProgress(1UL, 0UL, false));
        }
        finally
        {
            hooks.Release();
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
    }

    /// <summary>A write the log refuses because it holds a higher term says nothing about the health of the log and is not counted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    [Test]
    public async Task StaleTermRefusalIsNotCounted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-stale");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await using var committer = await LeadAsync(registry, cancellationToken);
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That(committer.AppendProgress()).IsEqualTo(new ReplicaAppendProgress(0UL, 0UL, false));
    }

    private static async Task<ReplicaGroupCommitter> LeadAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var cache = new StubCache();
        var committer = CreateElectedCommitter(registry, "n2", new ScriptedGateway(), cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1"));
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        var state = registry.StateFor("n2");
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        _ = state.GrantAuthority(2UL);
        return committer;
    }

    private static FollowerLogOptions Options(IFollowerLogFaultHooks hooks) =>
        new() { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks };
}
