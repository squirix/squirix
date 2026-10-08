using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
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
        var table = new ReplicaLeaderTable(registry, "n1");
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
}
