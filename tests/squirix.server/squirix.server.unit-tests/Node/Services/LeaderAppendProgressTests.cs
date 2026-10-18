using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
    private const int LocalAppendFailedEventId = 4075;
    private const int RepeatedFaults = 3;
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A write whose log append throws counts as one failed append, leaves nothing in flight, and leaves the log refusing.</summary>
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

        _ = await Assert.That(healthy).IsEqualTo(new ReplicaAppendProgress(0UL, 0UL, 0UL, false, false));
        _ = await Assert.That(committer.AppendProgress()).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, 1UL, false, true));
    }

    /// <summary>A refusal marks the log refused, a plain failure keeps it so, and the next append that succeeds clears it.</summary>
    [Test]
    public async Task AppendClearsRefusal()
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

        _ = await Assert.That(refused).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, 1UL, false, true));
        _ = await Assert.That(failedAfter).IsEqualTo(new ReplicaAppendProgress(0UL, 2UL, 2UL, false, true));
        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(1UL, 2UL, 3UL, false, false));
    }

    /// <summary>
    /// A log that latched its failure refuses the append as not ready, which blocks the leader; the next write cannot even start, appends
    /// nothing, and the leader stays blocked.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    [Test]
    public async Task RefusedLogIsReported(CancellationToken cancellationToken)
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
        _ = await Assert.That(refused).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, 1UL, false, true));
        _ = await Assert.That(committer.AppendProgress().Refused).IsTrue();
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

            _ = await Assert.That(stuck).IsEqualTo(new ReplicaAppendProgress(0UL, 0UL, 1UL, true, false));
            _ = await Assert.That(write.IsCompleted).IsFalse();
            hooks.Release();
            await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            _ = await Assert.That(committer.AppendProgress()).IsEqualTo(new ReplicaAppendProgress(1UL, 0UL, 1UL, false, false));
        }
        finally
        {
            hooks.Release();
            await hooks.Exited.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
    }

    /// <summary>
    /// The failure of the local append is logged once: it latches the log, and the writes the latched log refuses afterwards add no
    /// further line.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LocalAppendFaultIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-fault-once");
        var hooks = new FailingFrameWriteHooks();
        var events = new EventRecordingLogger();
        await using var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken);
        await using var committer = await LeadAsync(registry, events, cancellationToken);

        hooks.Arm();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var first = events.Count(LocalAppendFailedEventId);
        for (var i = 0; i < 2; i++)
            _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken));

        _ = await Assert.That(first).IsEqualTo(1);
        _ = await Assert.That(events.Count(LocalAppendFailedEventId)).IsEqualTo(1);
        _ = await Assert.That(events.Find(LocalAppendFailedEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(committer.AppendProgress().Refused).IsTrue();
    }

    /// <summary>
    /// The report of a failed local append is logged once while the fault repeats, again when another fault type appears, and again when
    /// it returns after an append that succeeded.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FaultReportRestartsAfterChange(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-fault-dedup");
        var events = new EventRecordingLogger();
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await using var committer = await LeadAsync(registry, events, cancellationToken);

        for (var i = 0; i < RepeatedFaults; i++)
            ReportFailedAppend(committer, new IOException("Injected write failure."));

        var repeated = events.Count(LocalAppendFailedEventId);
        ReportFailedAppend(committer, new UnauthorizedAccessException("Injected access failure."));
        var otherType = events.Count(LocalAppendFailedEventId);
        committer.LocalAppends.Begin();
        committer.LocalAppends.End(ReplicaLocalAppends.Outcome.Appended);
        ReportFailedAppend(committer, new UnauthorizedAccessException("Injected access failure."));

        _ = await Assert.That(repeated).IsEqualTo(1);
        _ = await Assert.That(otherType).IsEqualTo(2);
        _ = await Assert.That(events.Count(LocalAppendFailedEventId)).IsEqualTo(3);
    }

    /// <summary>
    /// A log that latched its failure after a failed append refuses the next writes before any append; the refusal is still reported, so the
    /// election driver can see it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchedLogRefusalBeforeAppendIsReported(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-latched");
        var hooks = new FailingFrameWriteHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, Options(hooks), cancellationToken);
        await using var committer = await LeadAsync(registry, cancellationToken);

        hooks.Arm();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var failed = committer.AppendProgress();
        hooks.Disarm();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken));

        _ = await Assert.That(failed.Started).IsEqualTo(1UL);
        _ = await Assert.That(committer.AppendProgress().Started).IsEqualTo(2UL);
        _ = await Assert.That(committer.AppendProgress().Refused).IsTrue();
        _ = await Assert.That(committer.AppendProgress().Failed).IsGreaterThan(failed.Failed);
        _ = await Assert.That(hooks.Failures).IsEqualTo(1);
    }

    /// <summary>A write the log refuses because it holds a higher term says nothing about the health of the log and is not counted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    [Test]
    public async Task StaleTermRefusalIsNotCounted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-append-stale");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var events = new EventRecordingLogger();
        await using var committer = await LeadAsync(registry, events, cancellationToken);
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That(committer.AppendProgress()).IsEqualTo(new ReplicaAppendProgress(0UL, 0UL, 1UL, false, false));
        _ = await Assert.That(events.Count(LocalAppendFailedEventId)).IsEqualTo(0);
    }

    private static void ReportFailedAppend(ReplicaGroupCommitter committer, Exception error)
    {
        var failedBefore = committer.LocalAppends.Snapshot().Failed;
        committer.LocalAppends.Begin();
        committer.LocalAppends.End(ReplicaLocalAppends.Outcome.Failed);
        committer.ReportLocalAppendFault(error, failedBefore);
    }

    private static Task<ReplicaGroupCommitter> LeadAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken) =>
        LeadAsync(registry, NullLogger<ReplicaGroupCommitter>.Instance, cancellationToken);

    private static async Task<ReplicaGroupCommitter> LeadAsync(ReplicaGroupRegistry registry, ILogger<ReplicaGroupCommitter> logger, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var cache = new StubCache();
        var committer = CreateElectedBudgetCommitter(
            registry,
            "n2",
            new ScriptedGateway(),
            (cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1")),
            TimeProvider.System,
            logger);
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
