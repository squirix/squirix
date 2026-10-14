using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A wait for this node's own authority over a group it leads ends with the authority, or at once when the node can no longer get it.</summary>
public sealed class ReplicaLeaderTableAuthorityTests : ServerUnitTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly IReplicaGroupLocator Ring = OwnerRouters.Locator("n1", "n2", "n3");

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(2);

    /// <summary>A leader waiting for its authority gets it as soon as its leader-term entry is committed, long before the timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitEndsWithAuthority(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-wait-granted");
        await using var registry = await ReplicaLeaderTableTests.OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = Leading(registry, 4UL);
        var wait = table.WaitForAuthorityAsync("n1", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        _ = state.GrantAuthority(4UL);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
    }

    /// <summary>A higher term seen while waiting ends the wait at once without authority: this node will not get it in its term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermEndsWait(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-wait-higher");
        await using var registry = await ReplicaLeaderTableTests.OpenTimedRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        var state = Leading(registry, 4UL);
        var wait = table.WaitForAuthorityAsync("n1", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        state.ObserveHigherTerm(5UL);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>Without authority, a wait ends as a timeout on the election clock, not earlier; a node that does not lead does not wait at all.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitTimesOutOnFakeClock(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-wait-timeout");
        var time = new FakeTimeProvider();
        await using var registry = await ReplicaLeaderTableTests.OpenTimedRegistryAsync(dir, time, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        _ = Leading(registry, 4UL);
        var follower = await table.WaitForAuthorityAsync("n2", Wait, cancellationToken);
        var wait = table.WaitForAuthorityAsync("n1", Wait, cancellationToken).AsTask();

        time.Advance(Wait - TimeSpan.FromMilliseconds(1));
        var pending = wait.IsCompleted;
        time.Advance(TimeSpan.FromMilliseconds(1));

        _ = await Assert.That((follower, pending)).IsEqualTo((false, false));
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsFalse();
    }

    /// <summary>Makes this node the leader of its own group, without authority yet.</summary>
    /// <param name="registry">The registry serving the group.</param>
    /// <param name="term">The won term.</param>
    /// <returns>The election state of the group.</returns>
    private static ReplicaGroupState Leading(ReplicaGroupRegistry registry, ulong term)
    {
        var state = registry.StateFor("n1");
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(term);
        return state;
    }
}
