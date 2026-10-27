using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>The runtime registration picks the dedupe authority of mutating RPCs from the same flag that selects replicated commits.</summary>
[Immutable]
public sealed class IdempotencyCoordinatorCompositionTests : DisposableServerUnitTestBase
{
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <summary>A host where network replication is activated resolves the coordinator that leaves deduplication to the replica group.</summary>
    [Test]
    public async Task ActivatedHostResolvesGroupCoordinator()
    {
        await using var provider = BuildProvider(FeatureState.Activated);

        var coordinator = provider.GetRequiredService<IRpcMutationIdempotencyCoordinator>();

        _ = await Assert.That(coordinator).IsTypeOf<ReplicatedMutationCoordinator>();
    }

    /// <summary>A single-copy host resolves the coordinator backed by the node-wide replay store.</summary>
    [Test]
    public async Task SingleCopyHostResolvesStoreCoordinator()
    {
        await using var provider = BuildProvider(FeatureState.Disabled);

        var coordinator = provider.GetRequiredService<IRpcMutationIdempotencyCoordinator>();

        _ = await Assert.That(coordinator).IsTypeOf<RpcMutationIdempotencyCoordinator>();
    }

    /// <summary>A host that only maps the replication transport keeps the single-copy path, as it does for the cache pipeline.</summary>
    [Test]
    public async Task FoundationHostResolvesStoreCoordinator()
    {
        await using var provider = BuildProvider(FeatureState.Foundation);

        var coordinator = provider.GetRequiredService<IRpcMutationIdempotencyCoordinator>();

        _ = await Assert.That(coordinator).IsTypeOf<RpcMutationIdempotencyCoordinator>();
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private ServiceProvider BuildProvider(FeatureState state)
    {
        var services = new ServiceCollection().AddLogging()
                                              .AddSingleton(_testMeter)
                                              .AddSingleton(new IdempotencyOptions())
                                              .AddSingleton(new TopologyOptions(new ServerPeer { NodeId = Self, Uri = new Uri("https://localhost:6001") }) { ClusterId = "c", NodeId = Self, Uri = new Uri("https://localhost:6001") })
                                              .AddSquirixRuntimeServices();

        // AddSingleton<T>(T) is constrained to class, so the descriptor boxes the state.
        services.Add(new ServiceDescriptor(typeof(FeatureState), _ => state, ServiceLifetime.Singleton));
        return services.BuildServiceProvider();
    }
}
