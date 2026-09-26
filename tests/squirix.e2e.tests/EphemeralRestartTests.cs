using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Cluster;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Verifies ephemeral nodes do not restore cache state across restart.</summary>
[Immutable]
public sealed class EphemeralRestartTests : EndToEndTestBase
{
    /// <summary>Ensures a restarted ephemeral node does not restore previously written values.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EphemeralModeDropsValuesOnRestart(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<string>("ephemeral-restart", cancellationToken: cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);

        cache = await cluster.GetCacheAsync<string>("ephemeral-restart", cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(result.Found).IsFalse();
    }
}
