using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// A restarted group owner probes its followers right away; stopping it while those probes are still in their TLS handshakes must end the
/// outbound handshakes before the node frees its certificate material, so the stop returns and the process survives.
/// </summary>
public sealed class NodeStopDuringVerificationTests : NodeIntegrationTestBase
{
    private const string CacheName = "node-stop-verification";
    private const int Iterations = 3;
    private const string OwnerId = "node-a";
    private const string SurvivorId = "node-b";

    /// <summary>Stopping the owner immediately after each restart, with the replica probes mid-handshake, returns every time and leaves the rest of the cluster writable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StopWhileProbesHandshakeReturns(CancellationToken cancellationToken)
    {
        const string scope = "node-stop-verification";
        await using var cluster = await StartClusterAsync(OwnerId, SurvivorId, "node-c", Options(scope, true), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[OwnerId], cancellationToken);

        for (var i = 0; i < Iterations; i++)
        {
            _ = await cluster.RestartNodeAsync(OwnerId, Options(scope, false), cancellationToken);
            await cluster.StopNodeAsync(OwnerId);
        }

        _ = await cluster.StartNodeAsync(OwnerId, Options(scope, false), cancellationToken);
        var survivor = cluster[SurvivorId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(survivor, cancellationToken);
        var key = survivor.FindKeyOwnedBy(CacheName, SurvivorId);
        var entry = new NodeCacheEntry<object?> { Value = "after-stops", Version = 1 };

        await survivor.GetCache<object?>(CacheName).SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, entry, cancellationToken);

        var stored = await survivor.GetCache<object?>(CacheName).GetEntryAsync(CacheName, key, cancellationToken);
        _ = await Assert.That(stored?.Value).IsEqualTo("after-stops").Because("The cluster must stay writable after the owner was stopped mid-handshake.");
    }

    private static IntegrationStartOptions Options(string scope, bool clean) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
    };
}
