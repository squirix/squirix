using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>The leader table of a node whose groups no election leads reproduces static ownership.</summary>
[Immutable]
public sealed class StaticLeaderTableTests : ServerUnitTestBase
{
    /// <summary>The ring owner of every group leads it in term one; this node has authority over its own group only.</summary>
    [Test]
    public async Task OwnerIsLeaderInTermOne()
    {
        var table = new StaticLeaderTable("n1");

        _ = await Assert.That((table.HasLocalAuthority("n1", out var own), own)).IsEqualTo((true, 1UL));
        _ = await Assert.That((table.HasLocalAuthority("n2", out var other), other)).IsEqualTo((false, 0UL));
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n2", 1UL)));
        _ = await Assert.That(table.Read("n1")).IsEqualTo(new GroupLeaderView(true, true, false, 1UL, 1UL, new LeaderRoute("n1", 1UL)));
        _ = await Assert.That(table.Read("n2")).IsEqualTo(new GroupLeaderView(true, false, false, 1UL, 1UL, new LeaderRoute("n2", 1UL)));
    }

    /// <summary>A leader is always known, so the wait returns at once, and a refutation changes nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitReturnsAtOnce(CancellationToken cancellationToken)
    {
        var table = new StaticLeaderTable("n1");
        table.Refute("n2", new LeaderRoute("n2", 1UL));

        var wait = table.WaitForLeaderAsync("n2", TimeSpan.FromHours(1), cancellationToken);

        _ = await Assert.That((wait.IsCompleted, await wait)).IsEqualTo((true, true));
        _ = await Assert.That((table.TryGetLeader("n2", out var route), route)).IsEqualTo((true, new LeaderRoute("n2", 1UL)));
    }

    /// <summary>
    /// The cluster locator registers the static table as the fallback; an election-led host registered afterwards replaces it, and a table
    /// registered before the locator is kept.
    /// </summary>
    [Test]
    public async Task LocatorRegistersFallbackTable()
    {
        var cluster = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") }) { NodeId = "n1" };
        var election = new StaticLeaderTable("other");
        var replaced = new ServiceCollection().AddSquirixClusterLocator(cluster).Replace(ServiceDescriptor.Singleton<IGroupLeaderTable>(election));
        await using var fallback = new ServiceCollection().AddSquirixClusterLocator(cluster).BuildServiceProvider();
        await using var elected = replaced.BuildServiceProvider();
        await using var kept = new ServiceCollection().AddSingleton<IGroupLeaderTable>(election).AddSquirixClusterLocator(cluster).BuildServiceProvider();

        _ = await Assert.That(fallback.GetRequiredService<IGroupLeaderTable>().HasLocalAuthority("n1", out _)).IsTrue();
        _ = await Assert.That(elected.GetRequiredService<IGroupLeaderTable>()).IsSameReferenceAs(election);
        _ = await Assert.That(elected.GetServices<IGroupLeaderTable>()).Count().IsEqualTo(1);
        _ = await Assert.That(kept.GetRequiredService<IGroupLeaderTable>()).IsSameReferenceAs(election);
    }
}
