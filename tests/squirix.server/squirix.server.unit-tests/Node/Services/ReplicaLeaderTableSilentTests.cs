using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The leader table stops naming a follower's leader once it went silent for one election timeout, and names it again on a contact.</summary>
public sealed class ReplicaLeaderTableSilentTests : ServerUnitTestBase
{
    private static readonly string[] Groups = ["n1", "n2", "n3"];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private static readonly IReplicaGroupLocator Ring = OwnerRouters.Locator(Groups);

    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(2);

    /// <summary>A wait that began while the known leader was silent ends when a contact from that leader revives it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitEndsWhenSilentLeaderReturns(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-wait-silent");
        var time = new FakeTimeProvider();
        await using var registry = await ReplicaLeaderTableTests.OpenTimedRegistryAsync(dir, time, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        registry.StateFor("n2").ObserveLeaderContact("n3", 3UL);
        time.Advance(registry.Election.ElectionTimeout);
        var wait = table.WaitForLeaderAsync("n2", Wait, cancellationToken).AsTask();
        var pending = wait.IsCompleted;

        registry.StateFor("n2").ObserveLeaderContact("n3", 3UL);

        _ = await Assert.That(pending).IsFalse();
        _ = await Assert.That(await wait.WaitAsync(HangGuard, TimeProvider.System, cancellationToken)).IsTrue();
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n3", 3UL)));
    }

    /// <summary>A leader not heard from for one election timeout is no leader, while the view of the group is still served.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SilentLeaderIsNoLeader(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-silent");
        var time = new FakeTimeProvider();
        await using var registry = await ReplicaLeaderTableTests.OpenTimedRegistryAsync(dir, time, cancellationToken);
        var table = new ReplicaLeaderTable(registry, "n1", Ring);
        registry.StateFor("n2").ObserveLeaderContact("n3", 3UL);

        time.Advance(registry.Election.ElectionTimeout);

        _ = await Assert.That(table.TryGetLeader("n2", out var route)).IsFalse();
        _ = await Assert.That(route).IsEqualTo(default);
        _ = await Assert.That(table.Read("n2")).IsEqualTo(new GroupLeaderView(true, false, false, 0UL, 3UL, default));
    }
}
