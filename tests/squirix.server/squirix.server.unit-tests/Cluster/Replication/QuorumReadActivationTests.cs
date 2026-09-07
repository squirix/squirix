using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Architecture;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Quorum reads stay disabled: reads are served locally without consulting a majority.</summary>
[Immutable]
public sealed class QuorumReadActivationTests : ServerUnitTestBase
{
    /// <summary>Release hosting exposes no quorum-read or failover switches before the proof matrix.</summary>
    [Fact]
    public async Task ReleaseHostCannotEnableBeforeProofMatrix()
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var productHost = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "AspNetCoreExtensions.cs"), DefaultCancellationToken);
        Assert.DoesNotContain("QuorumRead", productHost, StringComparison.Ordinal);
        Assert.DoesNotContain("AutomaticFailover", productHost, StringComparison.Ordinal);

        var optionsType = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "SquirixServerOptions.cs"), DefaultCancellationToken);
        Assert.DoesNotContain("QuorumRead", optionsType, StringComparison.Ordinal);
        Assert.DoesNotContain("AutomaticFailover", optionsType, StringComparison.Ordinal);
    }

    /// <summary>With quorum reads disabled, an RF=3 current read without quorum confirmation is rejected.</summary>
    [Fact]
    public void DisabledFlagRejectsRfThreeCurrentRead()
    {
        var rejected = LeaderAuthorityGate.CheckRead(3, true, true, 7, 7, new LeaderReadState(false, 9, 9));
        Assert.False(rejected.Allowed);
        Assert.Equal(LeaderAuthorityDenial.QuorumNotConfirmed, rejected.Denial);
    }

    /// <summary>RF=3 reads are served locally even after every follower stops.</summary>
    [Fact]
    public async Task RfThreeReadsRemainDisabled()
    {
        var uriA = ListenPortPool.ServerUnitTests.NextHttpUri();
        var uriB = ListenPortPool.ServerUnitTests.NextHttpUri();
        var uriC = ListenPortPool.ServerUnitTests.NextHttpUri();
        var peers = new[] { ("nodeA", uriA), ("nodeB", uriB), ("nodeC", uriC) };
        using var mtls = new ClusterTls();
        using var root = new TempDirectory("squirix-quorum-read");
        var options = new Func<string, TestNodeHostStartOptions>(node => new TestNodeHostStartOptions
        {
            ReplicaCount = 3,
            DataDir = NodePathKit.Combine(root.Path, node),
        });

        await using var nodeA = await TestNodeHostFactory.StartNodeAsync("nodeA", uriA, peers, options("nodeA"), mtls, DefaultCancellationToken);
        await using var nodeB = await TestNodeHostFactory.StartNodeAsync("nodeB", uriB, peers, options("nodeB"), mtls, DefaultCancellationToken);
        await using var nodeC = await TestNodeHostFactory.StartNodeAsync("nodeC", uriC, peers, options("nodeC"), mtls, DefaultCancellationToken);

        var cache = nodeA.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>("quorum-read");
        var key = FindKeyOwnedBy(nodeA, "quorum-read", "nodeA");
        await cache.SetEntryAsync(Guid.NewGuid().ToString(), "quorum-read", key, new NodeCacheEntry<object?> { Value = "v" }, DefaultCancellationToken);

        await nodeC.DisposeAsync();
        var majorityRead = await cache.GetValueAsync("quorum-read", key, DefaultCancellationToken);
        Assert.True(majorityRead.Found);

        // No majority remains, yet the read is still served locally: no quorum gate is consulted.
        await nodeB.DisposeAsync();
        var loneRead = await cache.GetValueAsync("quorum-read", key, DefaultCancellationToken);
        Assert.True(loneRead.Found);
        Assert.Equal("v", Assert.IsType<string>(loneRead.Value));
    }

    private static string FindKeyOwnedBy(TestNodeHost host, string cacheName, string owner)
    {
        var locator = host.Services.GetRequiredService<INodeLocator>();
        for (var i = 0; i < 10_000; i++)
        {
            var candidate = $"quorum-read-{i}";
            if (string.Equals(locator.GetOwner(cacheName, candidate), owner, StringComparison.Ordinal))
                return candidate;
        }

        throw new InvalidOperationException($"No key owned by '{owner}' was found.");
    }
}
