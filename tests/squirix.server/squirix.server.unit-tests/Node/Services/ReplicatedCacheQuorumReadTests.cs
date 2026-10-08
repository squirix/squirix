using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
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
/// Reads of the replicated cache under quorum reads: node n1 leads group n2 by election in term 2, from slot 2, with followers n2 and n3,
/// and key b belongs to group n2. A fenced read is served only once a majority confirmed its read index and memory applied it.
/// </summary>
public sealed class ReplicatedCacheQuorumReadTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly ElectionTimerOptions Timing = new() { JitterSeed = 1UL };

    /// <summary>Without quorum reads a read stays local even where no leader has authority; with them the same read is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QuorumReadsOffReadsLocally(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-quorum-read-off");
        await using var registry = await OpenAsync(dir, new FakeTimeProvider(), cancellationToken);
        var cache = new StubCache();
        await using var committers = Lead(registry, new ScriptedGateway(), cache);
        await cache.SetEntryAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);

        var local = await new ReplicatedCache(cache, committers).GetEntryAsync(CacheName, "b", cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<object?>?>(new ReplicatedCache(cache, committers, true).GetEntryAsync(CacheName, "b", cancellationToken));

        _ = await Assert.That(local?.Value).IsEqualTo("b");
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
    }

    /// <summary>
    /// An entry committed but not applied yet holds a confirmed read back: the read is served only after memory applied the read index,
    /// which the next write does first.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadWaitsForAppliedIndex(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var dir = new TempDirectory("squirix-quorum-read-applied");
        await using var registry = await OpenAsync(dir, time, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        await using var committers = Lead(registry, new ScriptedGateway(), cache, applier);
        var committer = await AuthorizeAsync(registry, committers, cancellationToken);
        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        var appliedBefore = applier.AppliedIndex;

        // Memory keeps refusing until the read waits, so no retry of the apply in the background can run ahead of it.
        var reading = new ReplicatedCache(cache, committers, true).GetEntryAsync(CacheName, "b", cancellationToken).AsTask();
        var waited = !reading.IsCompleted;
        cache.OnApplied = null;
        await committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken);
        await PollAsync(time, reading);
        var read = await reading.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That((appliedBefore, waited, applier.AppliedIndex)).IsEqualTo((1UL, true, 3UL));
        _ = await Assert.That(read?.Value).IsEqualTo("b");
    }

    /// <summary>A leader that sees a higher term while its read waits for the read index to be applied refuses it as stale-term once memory applied it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermDuringApplyRefusesRead(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var dir = new TempDirectory("squirix-quorum-read-deposed");
        await using var registry = await OpenAsync(dir, time, cancellationToken);
        var cache = new StubCache();
        await using var committers = Lead(registry, new ScriptedGateway(), cache);
        var committer = await AuthorizeAsync(registry, committers, cancellationToken);
        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        var reading = new ReplicatedCache(cache, committers, true).GetEntryAsync(CacheName, "b", cancellationToken).AsTask();
        var waited = !reading.IsCompleted;
        cache.OnApplied = null;
        await committer.CommitSetAsync(NewOperationId(), CacheName, "c", Entry("c"), cancellationToken);
        registry.StateFor("n2").ObserveHigherTerm(3UL);
        await PollAsync(time, reading);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(reading.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(waited).IsTrue();
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
    }

    /// <summary>A read waiting for its round fails at once, as unconfirmed, when the leadership retires and closes the pipeline of the round.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetiredLeaderFailsPendingRead(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-quorum-read-retired");
        await using var registry = await OpenAsync(dir, new FakeTimeProvider(), cancellationToken);
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var committers = Lead(registry, gateway, cache);
        _ = await AuthorizeAsync(registry, committers, cancellationToken);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);

        var reading = new ReplicatedCache(cache, committers, true).GetEntryAsync(CacheName, "b", cancellationToken).AsTask();
        var waited = !reading.IsCompleted;
        var retired = await committers.RetireAsync("n2", cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(reading.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That((waited, retired)).IsEqualTo((true, true));
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ReadQuorumUnconfirmedDetail));
    }

    /// <summary>A leader whose followers stop answering refuses a read once the election timeout passed without a majority confirming its read index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnconfirmedRoundRefusesRead(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var dir = new TempDirectory("squirix-quorum-read-unconfirmed");
        await using var registry = await OpenAsync(dir, time, cancellationToken);
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var committers = Lead(registry, gateway, cache);
        _ = await AuthorizeAsync(registry, committers, cancellationToken);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);

        var reading = new ReplicatedCache(cache, committers, true).GetEntryAsync(CacheName, "b", cancellationToken).AsTask();
        var waited = !reading.IsCompleted;
        time.Advance(Timing.ElectionTimeout);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(reading.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(waited).IsTrue();
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ReadQuorumUnconfirmedDetail));
    }

    /// <summary>Promotes node n1 to lead group n2 in term 2 and grants it authority, as the election driver does once the leader-term entry commits.</summary>
    /// <param name="registry">The registry of node n1.</param>
    /// <param name="committers">The committers of node n1.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The committer of group n2.</returns>
    /// <exception cref="InvalidOperationException">The group log is not open, or the promotion was not authorized.</exception>
    private static async Task<ReplicaGroupCommitter> AuthorizeAsync(ReplicaGroupRegistry registry, ReplicaGroupCommitters committers, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var state = registry.StateFor("n2");
        state.SetElectionDriven(true);
        var authorized = await committers.PromoteAsync("n2", 2UL, cancellationToken) && state.BecomeLeader(2UL) && state.GrantAuthority(2UL);
        return authorized ? committers.For("n2") : throw new InvalidOperationException("Node n1 must lead group n2 with authority in term 2.");
    }

    /// <summary>Moves the fake clock a millisecond at a time until a read waiting on the applied-index poll completes, staying well inside its bound.</summary>
    /// <param name="time">The fake clock of the poll.</param>
    /// <param name="reading">The read.</param>
    /// <returns>A task that completes once the read completed or the steps ran out.</returns>
    private static async Task PollAsync(FakeTimeProvider time, Task reading)
    {
        for (var step = 0; step < 100 && !reading.IsCompleted; step++)
        {
            time.Advance(TimeSpan.FromMilliseconds(1));
            await Task.Yield();
        }
    }

    private static ReplicaGroupCommitters Lead(ReplicaGroupRegistry registry, ScriptedGateway gateway, StubCache cache, ReplicaGroupApplier? applier = null) =>
        new(
            groupId => CreateElectedCommitter(registry, groupId, gateway, cache, applier ?? new ReplicaGroupApplier(cache, NullLogger.Instance, groupId, "n1")),
            new ReplicaLeaderTable(registry, "n1"),
            "n1",
            Owners(),
            TimeProvider.System);

    private static async Task<ReplicaGroupRegistry> OpenAsync(TempDirectory dir, TimeProvider time, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1UL, NullLoggerFactory.Instance)
        {
            Election = Timing,
            ElectionClock = time,
            QuorumReads = true,
        };

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
