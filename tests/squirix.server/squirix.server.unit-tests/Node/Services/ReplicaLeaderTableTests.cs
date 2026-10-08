using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The leader table follows the election state of each group: authority and known leaders appear and vanish with it.</summary>
public sealed class ReplicaLeaderTableTests : ServerUnitTestBase
{
    private static readonly string[] Groups = ["n1", "n2", "n3"];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan[] InvalidWaits =
        [TimeSpan.Zero, TimeSpan.FromMilliseconds(-5), Timeout.InfiniteTimeSpan, ElectionTimerOptions.MaxLeaderWaitTimeout + TimeSpan.FromTicks(1)];

    private static readonly IReplicaGroupLocator Ring = OwnerRouters.Locator(Groups);

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(2);

    /// <summary>
    /// A leader has local authority only once its leader-term entry is committed, and loses it at once to a higher term; a follower routes to
    /// the leader it accepted contact from; a group this node does not serve has no route.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TableFollowsElectionState(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-table");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var led = registry.StateFor("n2");
        led.SetElectionDriven(true);
        _ = led.BecomeLeader(2UL);
        var beforeCommit = table.HasLocalAuthority("n2", out _);
        _ = led.GrantAuthority(2UL);
        var authorized = (table.HasLocalAuthority("n2", out var term), term, table.TryGetLeader("n2", out var own), own);
        led.ObserveHigherTerm(3UL);
        var deposed = table.HasLocalAuthority("n2", out _);
        registry.StateFor("n3").ObserveLeaderContact("n3", 4UL);

        _ = await Assert.That(beforeCommit).IsFalse();
        _ = await Assert.That(authorized).IsEqualTo((true, 2UL, true, new LeaderRoute("n1", 2UL)));
        _ = await Assert.That(deposed).IsFalse();
        _ = await Assert.That((table.TryGetLeader("n3", out var followed), followed)).IsEqualTo((true, new LeaderRoute("n3", 4UL)));
        _ = await Assert.That(table.TryGetLeader("n1", out _)).IsFalse();
        _ = await Assert.That(table.TryGetLeader("n4", out _) || table.HasLocalAuthority("n4", out _)).IsFalse();
    }

    /// <summary>A deposed leader reads as led without authority, with the higher term it observed and no leader to route to.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeposedLeaderReadsWithoutAuthority(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-table-deposed");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var led = registry.StateFor("n2");
        led.SetElectionDriven(true);
        _ = led.BecomeLeader(2UL);
        _ = led.GrantAuthority(2UL);

        led.ObserveHigherTerm(3UL);

        _ = await Assert.That(table.Read("n2")).IsEqualTo(new GroupLeaderView(true, false, true, 2UL, 3UL, default));
    }

    /// <summary>A wait for a leader ends as soon as a follower accepts a leader contact, long before its timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitEndsOnLeaderContact(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-contact");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var wait = table.WaitForLeaderAsync("n2", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        registry.StateFor("n2").ObserveLeaderContact("n2", 3UL);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n2", 3UL)));
    }

    /// <summary>A wait for a leader ends when this node gains authority, and names this node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitEndsOnLocalAuthority(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-authority");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = registry.StateFor("n1");
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(4UL);
        var wait = table.WaitForLeaderAsync("n1", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        _ = state.GrantAuthority(4UL);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That((table.TryGetLeader("n1", out var route), route)).IsEqualTo((true, new LeaderRoute("n1", 4UL)));
    }

    /// <summary>A refuted route is not a leader: the wait skips it and ends only when the state reports another route.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitSkipsRefutedRoute(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-refuted");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = registry.StateFor("n2");
        state.ObserveLeaderContact("n2", 3UL);
        table.Refute("n2", new LeaderRoute("n2", 3UL));
        var wait = table.WaitForLeaderAsync("n2", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        state.ObserveLeaderContact("n3", 4UL);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n3", 4UL)));
    }

    /// <summary>A wait without any leader ends as a timeout on the election clock, not earlier.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitTimesOutOnFakeClock(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-timeout");
        var time = new FakeTimeProvider();
        await using var registry = await OpenTimedRegistryAsync(dir, time, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var wait = table.WaitForLeaderAsync("n2", Wait, cancellationToken).AsTask();
        time.Advance(Wait - TimeSpan.FromMilliseconds(1));
        var early = wait.IsCompleted;

        time.Advance(TimeSpan.FromMilliseconds(1));

        _ = await Assert.That(early).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>The same leader in a newer term is a different route, so a refutation of its older term no longer hides it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefutedRouteReturnsAfterNewTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-refuted-term");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = registry.StateFor("n2");
        state.ObserveLeaderContact("n2", 3UL);
        table.Refute("n2", new LeaderRoute("n2", 3UL));
        var hidden = table.TryGetLeader("n2", out _);

        state.ObserveLeaderContact("n2", 5UL);

        _ = await Assert.That(hidden).IsFalse();
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n2", 5UL)));
    }

    /// <summary>Own authority is never refuted; only the election state revokes it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnAuthorityIsNeverRefuted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-refuted-self");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = registry.StateFor("n1");
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        _ = state.GrantAuthority(2UL);

        table.Refute("n1", new LeaderRoute("n1", 2UL));

        _ = await Assert.That((table.TryGetLeader("n1", out var route), route)).IsEqualTo((true, new LeaderRoute("n1", 2UL)));
    }

    /// <summary>A group this node does not serve has an unserved view, ignores refutations, and its wait ends at once without a leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnservedGroupHasNoRoute(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-unserved");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        table.Refute("n4", new LeaderRoute("n4", 1UL));

        var wait = table.WaitForLeaderAsync("n4", Wait, cancellationToken);

        _ = await Assert.That(table.Read("n4")).IsEqualTo(default);
        _ = await Assert.That((wait.IsCompleted, await wait)).IsEqualTo((true, false));
    }

    /// <summary>The wait for a leader defaults to the longest time a follower waits before it campaigns.</summary>
    [Test]
    public async Task WaitDefaultsToElectionBound()
    {
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(300), MaxJitter = TimeSpan.FromMilliseconds(200) };
        var pinned = new ElectionTimerOptions { LeaderWaitTimeoutOverride = TimeSpan.FromMilliseconds(50) };

        _ = await Assert.That(new ElectionTimerOptions().LeaderWaitTimeout).IsEqualTo(TimeSpan.FromSeconds(2));
        _ = await Assert.That(options.LeaderWaitTimeout).IsEqualTo(TimeSpan.FromMilliseconds(500));
        _ = await Assert.That(pinned.LeaderWaitTimeout).IsEqualTo(TimeSpan.FromMilliseconds(50));
    }

    /// <summary>
    /// An explicit wait for a leader must be positive and bounded, an infinite one included, and the leader table refuses options that break
    /// this when it is built, so a bad value fails at startup.
    /// </summary>
    [Test]
    public async Task UnboundedWaitIsRefused()
    {
        using var dir = new TempDirectory("squirix-leader-wait-invalid");
        foreach (var value in InvalidWaits)
        {
            _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>()
                .Throws(new ElectionTimerOptions { LeaderWaitTimeoutOverride = value }, ElectionTimerOptions.EnsureValidLeaderWait);
        }

        await using var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1UL, NullLoggerFactory.Instance)
        {
            Election = new ElectionTimerOptions { LeaderWaitTimeoutOverride = Timeout.InfiniteTimeSpan },
        };
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(registry, static r => _ = new ReplicaLeaderTable(r, "n1", Ring));
        var bounded = new ElectionTimerOptions { LeaderWaitTimeoutOverride = ElectionTimerOptions.MaxLeaderWaitTimeout };
        ElectionTimerOptions.EnsureValidLeaderWait(bounded);

        _ = await Assert.That(bounded.LeaderWaitTimeout).IsEqualTo(ElectionTimerOptions.MaxLeaderWaitTimeout);
    }

    /// <summary>
    /// A refutation of a newer route does not spend an older one: the older route the state still reports stays hidden until a leader of a
    /// higher term than every refutation is known.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefutationKeepsHighestTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-refuted-highest");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = registry.StateFor("n2");
        state.ObserveLeaderContact("n2", 3UL);
        table.Refute("n2", new LeaderRoute("n2", 3UL));
        table.Refute("n2", new LeaderRoute("n3", 4UL));
        var first = table.TryGetLeader("n2", out _);
        table.Refute("n2", new LeaderRoute("n2", 3UL));
        var second = table.TryGetLeader("n2", out _);

        state.ObserveLeaderContact("n3", 4UL);
        var sameTerm = table.TryGetLeader("n2", out _);
        state.ObserveLeaderContact("n3", 5UL);

        _ = await Assert.That((first, second, sameTerm)).IsEqualTo((false, false, false));
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n3", 5UL)));
    }

    /// <summary>
    /// A leader hint is kept only for a group this node does not serve and only when it names a member of that group; the newest term wins, a
    /// refutation of the kept leader forgets it, and the election state of the group is untouched.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LearnKeepsNewestMemberHint(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-learned");
        await using var registry = await OpenRegistryAsync(dir, ["n1", "n4", "n5"], null, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", new ReplicaGroupLocator(new PhysicalNodeRing(["n1", "n2", "n3", "n4", "n5"]), 3));
        table.Learn("n2", new LeaderRoute("n3", 4UL));
        table.Learn("n2", new LeaderRoute("n4", 3UL));
        table.Learn("n2", new LeaderRoute("n5", 9UL));
        table.Learn("n2", new LeaderRoute("n1", 9UL));
        table.Learn("n1", new LeaderRoute("n2", 9UL));
        var kept = (table.TryGetLearnedLeader("n2", out var learned), learned);
        table.Learn("n2", new LeaderRoute("n4", 4UL));
        var newer = (table.TryGetLearnedLeader("n2", out var replaced), replaced);
        table.Refute("n2", new LeaderRoute("n3", 9UL));
        var other = table.TryGetLearnedLeader("n2", out _);
        table.Refute("n2", new LeaderRoute("n4", 4UL));

        _ = await Assert.That(kept).IsEqualTo((true, new LeaderRoute("n3", 4UL)));
        _ = await Assert.That(newer).IsEqualTo((true, new LeaderRoute("n4", 4UL)));
        _ = await Assert.That(other).IsTrue();
        _ = await Assert.That(table.TryGetLearnedLeader("n2", out _) || table.TryGetLearnedLeader("n1", out _)).IsFalse();
        _ = await Assert.That(table.TryGetLeader("n2", out _) || table.TryGetLeader("n1", out _)).IsFalse();
    }

    /// <summary>A member learned in term zero after it served a fallback gives way to a hint of any higher term, even a stale one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TermZeroGivesWayToHint(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-learned-zero");
        await using var registry = await OpenRegistryAsync(dir, ["n1", "n4", "n5"], null, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", new ReplicaGroupLocator(new PhysicalNodeRing(["n1", "n2", "n3", "n4", "n5"]), 3));
        table.Learn("n2", new LeaderRoute("n3", 0UL));
        table.Learn("n2", new LeaderRoute("n4", 2UL));

        _ = await Assert.That((table.TryGetLearnedLeader("n2", out var learned), learned)).IsEqualTo((true, new LeaderRoute("n4", 2UL)));
    }

    /// <summary>A canceled wait for a leader throws, an infinite one included.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledWaitThrows(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-cancel");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = table.WaitForLeaderAsync("n2", Timeout.InfiniteTimeSpan, cancel.Token);
        var pending = wait.IsCompleted;

        await cancel.CancelAsync();

        _ = await Assert.That(pending).IsFalse();
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, bool>(wait);
    }

    /// <summary>A candidate that falls back to follower while it still knows the leader wakes the waiter with that leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitWakesOnCandidateFollower(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-candidate");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = registry.StateFor("n2");
        state.ObserveLeaderContact("n3", 3UL);
        state.SetElectionDriven(true);
        state.BecomePreCandidate();
        state.BecomeCandidate(4UL);
        var wait = table.WaitForLeaderAsync("n2", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        state.BecomeFollower(4UL, false);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n3", 3UL)));
    }

    /// <summary>A stopped driver leaves the group a follower of the leader it knows, which wakes the waiter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitWakesWhenDriverStops(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-undriven");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = registry.StateFor("n2");
        state.ObserveLeaderContact("n3", 3UL);
        state.SetElectionDriven(true);
        state.BecomePreCandidate();
        var wait = table.WaitForLeaderAsync("n2", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        state.SetElectionDriven(false);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>A zero timeout checks the table once and never waits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ZeroTimeoutChecksOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-zero");
        await using var registry = await OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var none = table.WaitForLeaderAsync("n2", TimeSpan.Zero, cancellationToken);
        var noneResult = (none.IsCompleted, await none);
        registry.StateFor("n2").ObserveLeaderContact("n3", 3UL);

        var known = table.WaitForLeaderAsync("n2", TimeSpan.Zero, cancellationToken);

        _ = await Assert.That(noneResult).IsEqualTo((true, false));
        _ = await Assert.That((known.IsCompleted, await known)).IsEqualTo((true, true));
    }

    private static async Task<ReplicaGroupRegistry> OpenTimedRegistryAsync(TempDirectory dir, TimeProvider time, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1UL, NullLoggerFactory.Instance) { ElectionClock = time };
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
