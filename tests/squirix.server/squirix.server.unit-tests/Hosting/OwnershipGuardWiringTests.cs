using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
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
        var leaders = new IGroupLeaderTableCreateExpectations();
        _ = leaders.Setups.Read(Remote).ReturnValue(new GroupLeaderView(true, false, false, 6, 6, new LeaderRoute(Remote, 6)));

        var failure = await RefuseAsync(leaders.Instance(), cancellationToken);

        _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That((failure.FindTrailer("squirix-leader-node-id"), failure.FindTrailer("squirix-leader-term"))).IsEqualTo((Remote, "6"));
    }

    private static async Task<ServerCallException> RefuseAsync(IGroupLeaderTable leaders, CancellationToken cancellationToken)
    {
        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Remote);
        var below = new BackpressureCacheDecorator<object?>(
            new ILogicalNamespacedCacheCreateExpectations<object?>().Instance(),
            new IBackpressureGateCreateExpectations().Instance(),
            new IBackpressureClientIdResolverCreateExpectations().Instance());
        var services = new NodeServices().AddSquirixCachePipeline()
                                              .Use(new TopologyOptions(new ServerPeer { NodeId = Self, Uri = new Uri("https://localhost:6001") }) { ClusterId = "c", NodeId = Self, Uri = new Uri("https://localhost:6001") })
                                              .Use(locator.Instance())
                                              .Use(leaders)
                                              .Use(below);
        await using var provider = new NodeProvider(services);
        var guard = provider.Require<OwnershipGuardCacheDecorator<object?>>();

        return await NodeAsyncAssert.ThrowsAsync<ServerCallException>(SetAsync(guard, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        static async Task SetAsync(OwnershipGuardCacheDecorator<object?> guard, CancellationToken cancellationToken)
        {
            await guard.SetEntryAsync("op", "c", "k", new NodeCacheEntry<object?> { Value = "v" }, cancellationToken);
        }
    }
}
