using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Composition;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>The cache pipeline wires the ownership guard to name elected leaders in hint trailers only when the leader table is elected.</summary>
[Immutable]
public sealed class OwnershipGuardWiringTests : ServerUnitTestBase
{
    private const string Remote = "node-b";
    private const string Self = "node-a";

    /// <summary>With the static table the stale-owner refusal carries only its error code.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticTableWiresNoLeaderHints(CancellationToken cancellationToken)
    {
        var failure = await RefuseAsync(new StaticLeaderTable(Self), cancellationToken);

        _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(failure.Error.Trailers.Count).IsEqualTo(1);
    }

    /// <summary>With an elected table the stale-owner refusal names the known leader in the hint trailers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ElectedTableWiresLeaderHints(CancellationToken cancellationToken)
    {
        var leaders = IGroupLeaderTable.Mock(MockBehavior.Strict);
        _ = leaders.Read(Remote).Returns(new GroupLeaderView(true, false, false, 6, 6, new LeaderRoute(Remote, 6)));

        var failure = await RefuseAsync(leaders.Object, cancellationToken);

        _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That((failure.FindTrailer("squirix-leader-node-id"), failure.FindTrailer("squirix-leader-term"))).IsEqualTo((Remote, "6"));
    }

    /// <summary>A sole peer reaches admission without consulting an owner even outside an RPC context.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SolePeerKeepsLocalAdmission(CancellationToken cancellationToken)
    {
        var cluster = new TopologyOptions(new ServerPeer { NodeId = Self, Uri = new Uri("https://localhost:6001") }) { NodeId = Self };
        var gate = IBackpressureGate.Mock(MockBehavior.Strict);
        _ = gate.AcquireAsync(Arg.Any<string>(), Arg.Any<string>(), "client", cancellationToken)
                .ReturnsAsync(ValueTask.FromResult((Decision.Accepted(), Lease.Empty)));
        var clients = IBackpressureClientIdResolver.Mock(MockBehavior.Strict);
        _ = clients.Resolve().Returns("client");
        var entry = new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText("v") };
        var store = ILogicalNamespacedCache<StoredValue>.Mock(MockBehavior.Strict);
        _ = store.SetEntryAsync("op", "c", "k", entry, cancellationToken).ReturnsAsync(ValueTask.CompletedTask);
        var admission = new BackpressureCacheDecorator<StoredValue>(store.Object, gate.Object, clients.Object);
        var services = new NodeServices().AddSquirixClusterLocator(cluster).AddSquirixCachePipeline()
                                         .Use(FeatureState.Disabled)
                                         .Use(admission);
        await using var provider = new NodeProvider(services);

        await provider.Require<ValidationCacheDecorator<StoredValue>>().SetEntryAsync("op", "c", "k", entry, null, cancellationToken);

        store.SetEntryAsync("op", "c", "k", entry, cancellationToken).WasCalled(Times.Once);
        gate.AcquireAsync(Arg.Any<string>(), Arg.Any<string>(), "client", cancellationToken).WasCalled(Times.Once);
    }

    private static async Task<ServerCallException> RefuseAsync(IGroupLeaderTable leaders, CancellationToken cancellationToken)
    {
        var locator = INodeLocator.Mock(MockBehavior.Strict);
        _ = locator.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Returns(Remote);
        var below = new BackpressureCacheDecorator<StoredValue>(
            ILogicalNamespacedCache<StoredValue>.Mock(MockBehavior.Strict).Object,
            IBackpressureGate.Mock(MockBehavior.Strict).Object,
            IBackpressureClientIdResolver.Mock(MockBehavior.Strict).Object);
        var services = new NodeServices().AddSquirixCachePipeline()
                                              .Use(new TopologyOptions(new ServerPeer { NodeId = Self, Uri = new Uri("https://localhost:6001") }) { ClusterId = "c", NodeId = Self, Uri = new Uri("https://localhost:6001") })
                                              .Use(locator.Object)
                                              .Use(leaders)
                                              .Use(below);
        await using var provider = new NodeProvider(services);
        var guard = provider.Require<OwnershipGuardCacheDecorator<StoredValue>>();

        return await NodeAsyncAssert.ThrowsAsync<ServerCallException>(SetAsync(guard, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        static async Task SetAsync(OwnershipGuardCacheDecorator<StoredValue> guard, CancellationToken cancellationToken)
        {
            await guard.SetEntryAsync("op", "c", "k", new NodeCacheEntry<StoredValue> { Value = StoredValue.FromText("v") }, null, cancellationToken);
        }
    }
}
