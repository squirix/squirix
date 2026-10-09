using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// The cache pipeline a node resolves fences reads only where quorum reads meet leaders elected by automatic failover: an owner cut off
/// from its group refuses a read of its own key there, and serves it locally where it leads its group statically.
/// </summary>
public sealed class QuorumReadWiringTests : NodeIntegrationTestBase
{
    private const string Owner = "node-a";
    private const string Scope = "quorum-read-wiring";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly ElectionTimerOptions Timing = new()
    {
        ElectionTimeout = TimeSpan.FromSeconds(2),
        HeartbeatInterval = TimeSpan.FromMilliseconds(200),
        MaxJitter = TimeSpan.FromSeconds(2),
        VoteRpcTimeout = TimeSpan.FromSeconds(1),
    };

    private static readonly string[] Three = [Owner, "node-b", "node-c"];

    /// <summary>With failover and quorum reads, the resolved pipeline of an owner that holds no authority over its group refuses the read.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailoverReadsAreFenced(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(Options("quorum-wiring-failover", fabric, true), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Owner, Bound);
        var owner = cluster[Owner];
        var key = owner.FindKeyOwnedBy(Scope, Owner);

        await fabric.IsolateAsync(Owner);
        await ledger.UntilAsync(() => !owner.GetRequiredService<IGroupLeaderTable>().HasLocalAuthority(Owner, out _), "the cut-off owner holds no authority", cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<object?>?>(owner.GetCache<object?>(Scope).GetEntryAsync(Scope, key, cancellationToken));

        _ = await Assert.That(refused.StatusCode is StatusCode.Unavailable or StatusCode.FailedPrecondition).IsTrue();
    }

    /// <summary>With both switches off, the owner leads its group statically and its resolved pipeline reads locally, even cut off.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticLeaderReadsUnfenced(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        await using var cluster = await StartAsync(Options("quorum-wiring-static", fabric, false), cancellationToken);
        var owner = cluster[Owner];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var cache = owner.GetCache<object?>(Scope);
        var key = owner.FindKeyOwnedBy(Scope, Owner);
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), Scope, key, new NodeCacheEntry<object?> { Value = "v" }, cancellationToken);

        await fabric.IsolateAsync(Owner);
        var read = await cache.GetEntryAsync(Scope, key, cancellationToken);

        _ = await Assert.That(read?.Value).IsEqualTo("v");
    }

    private static IntegrationStartOptions Options(string scope, PartitionFabric fabric, bool failover) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        ExtraScope = scope,
        AutomaticFailoverEnabled = failover,
        QuorumReadsEnabled = failover,
        PartitionFabric = fabric,
        ServicesConfigure = static services => _ = services.AddSingleton(Timing),
    };

    private ValueTask<TestCluster<IntegrationStartOptions>> StartAsync(IntegrationStartOptions options, CancellationToken cancellationToken)
    {
        var topology = new ClusterNode[Three.Length];
        for (var i = 0; i < Three.Length; i++)
            topology[i] = new ClusterNode(Three[i], GetNextHttpUri());

        return StartClusterAsync(topology, options, cancellationToken);
    }
}
