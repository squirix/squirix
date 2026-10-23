using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A leader whose committed entries cannot be applied reports how far it is behind, retries the apply when it is not asked to write, and
/// logs the same fault once. Node n1 leads group n2 from slot 2; its followers are n2 and n3.
/// </summary>
public sealed class LeaderApplyProgressTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const int PendingApplyFailedEventId = 4007;
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A committed entry whose apply failed is reported as behind the majority without the commit gate, and once the apply recovers and a
    /// later write is applied, the applied index equals the majority index.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyProgressReportsCommittedUnapplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-progress");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await TermAsync(registry, 2UL, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        await using var committer = CreateElectedCommitter(registry, "n2", new ScriptedGateway(), cache, applier);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        var caughtUp = committer.ApplyProgress();

        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var blocked = committer.ApplyProgress();

        cache.OnApplied = null;
        await committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken);
        var recovered = committer.ApplyProgress();

        _ = await Assert.That(caughtUp).IsEqualTo(new ReplicaApplyProgress(1UL, 1UL));
        _ = await Assert.That((blocked, blocked.IsBehind)).IsEqualTo((new ReplicaApplyProgress(1UL, 2UL), true));
        _ = await Assert.That((recovered, recovered.IsBehind)).IsEqualTo((new ReplicaApplyProgress(3UL, 3UL), false));
    }

    /// <summary>
    /// The fault that keeps a committed entry from being applied is logged once while it repeats at the same applied index, and again when
    /// it returns after an apply that completed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PendingApplyFaultIsLoggedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-fault-once");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await TermAsync(registry, 2UL, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        var events = new EventRecordingLogger();
        await using var committer = CreateElectedBudgetCommitter(registry, "n2", new ScriptedGateway(), (cache, applier), TimeProvider.System, events);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        var first = await committer.TryApplyPendingAsync();
        var second = await committer.TryApplyPendingAsync();
        var third = await committer.TryApplyPendingAsync();
        var repeated = events.Count(PendingApplyFailedEventId);

        cache.OnApplied = null;
        var recovered = await committer.TryApplyPendingAsync();
        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken));
        _ = await committer.TryApplyPendingAsync();
        _ = await committer.TryApplyPendingAsync();

        _ = await Assert.That((first, second, third, recovered)).IsEqualTo((false, false, false, true));
        _ = await Assert.That(repeated).IsEqualTo(1);
        _ = await Assert.That(events.Count(PendingApplyFailedEventId)).IsEqualTo(2);
    }

    /// <summary>A readiness pass applies the committed entry a failed apply left behind, so a leader nobody writes to catches its memory up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerificationRetriesCommittedApply(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-verification");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await TermAsync(registry, 2UL, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        await using var committer = CreateElectedCommitter(registry, "n2", new ScriptedGateway(), cache, applier);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var blocked = committer.ApplyProgress();

        cache.OnApplied = null;
        _ = await committer.VerifyReplicasAsync(cancellationToken);

        _ = await Assert.That(blocked.IsBehind).IsTrue();
        _ = await Assert.That(committer.ApplyProgress()).IsEqualTo(new ReplicaApplyProgress(2UL, 2UL));
    }

    /// <summary>The same fault type at the same applied index is logged once, and a fault of another type at that index is logged again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherFaultTypeIsLoggedAgain(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-fault-type");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await TermAsync(registry, 2UL, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        var events = new EventRecordingLogger();
        await using var committer = CreateElectedBudgetCommitter(registry, "n2", new ScriptedGateway(), (cache, applier), TimeProvider.System, events);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        _ = await committer.TryApplyPendingAsync();
        _ = await committer.TryApplyPendingAsync();
        var sameType = events.Count(PendingApplyFailedEventId);

        cache.OnApplied = static () => throw new InvalidOperationException("another fault");
        _ = await committer.TryApplyPendingAsync();
        _ = await committer.TryApplyPendingAsync();

        _ = await Assert.That(sameType).IsEqualTo(1);
        _ = await Assert.That(events.Count(PendingApplyFailedEventId)).IsEqualTo(2);
    }

    /// <summary>A write that lost its majority after the local append leaves nothing a majority backs: the leader is not behind.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoMajorityIsNotBehind(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-no-majority");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await TermAsync(registry, 2UL, cancellationToken);
        var cache = new StubCache();
        var gateway = new ScriptedGateway();
        await using var committer = CreateElectedCommitter(registry, "n2", gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1"));
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var progress = committer.ApplyProgress();

        _ = await Assert.That(progress.IsBehind).IsFalse();
        _ = await Assert.That(progress).IsEqualTo(new ReplicaApplyProgress(1UL, 1UL));
    }

    /// <summary>The progress is read at once while a stalled apply holds the commit gate and the commit gate of the coordinator.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProgressIsReadWhileApplyIsStalled(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-progress-stalled");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await TermAsync(registry, 2UL, cancellationToken);
        var cache = new ScriptedApplyCache(ApplyMode.Stall);
        await using var committer = CreateElectedCommitter(registry, "n2", new ScriptedGateway(), cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1"));
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, 2UL);
        try
        {
            var write = committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
            await cache.ApplyEntered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

            var progress = committer.ApplyProgress();

            _ = await Assert.That(progress).IsEqualTo(new ReplicaApplyProgress(1UL, 2UL));
            _ = await Assert.That(write.IsCompleted).IsFalse();
            cache.ReleaseApply();
            await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            cache.ReleaseApply();
        }
    }

    private static void GrantAuthority(ReplicaGroupRegistry registry, ulong term)
    {
        var state = registry.StateFor("n2");
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(term);
        _ = state.GrantAuthority(term);
    }

    private static async Task TermAsync(ReplicaGroupRegistry registry, ulong term, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(term, cancellationToken);
    }
}
