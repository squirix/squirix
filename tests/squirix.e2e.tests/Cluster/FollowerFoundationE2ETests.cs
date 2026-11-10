using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>End-to-end coverage for reading persisted entries after restart.</summary>
public sealed class FollowerFoundationE2ETests
{
    /// <summary>Both persisted entries remain readable through the SDK after a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresPersistedEntries(CancellationToken cancellationToken)
    {
        const string name = nameof(RestartRestoresPersistedEntries);
        await using var cluster = await HostedCluster.StartSingleNodeAsync(name, persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<string>("persisted-entries", cancellationToken: cancellationToken);
        await cache.SetAsync("committed", "baseline", cancellationToken: cancellationToken);
        await cache.SetAsync("tail", "journal", cancellationToken: cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restartedCache = await cluster.GetCacheAsync<string>("persisted-entries", cancellationToken: cancellationToken);
        var first = await restartedCache.GetValueAsync("committed", cancellationToken);
        var second = await restartedCache.GetValueAsync("tail", cancellationToken);

        _ = await Assert.That(first.Found).IsTrue().Because("The first entry was not restored after the restart.");
        _ = await Assert.That(first.Value).IsEqualTo("baseline");
        _ = await Assert.That(second.Found).IsTrue().Because("The second entry was not restored after the restart.");
        _ = await Assert.That(second.Value).IsEqualTo("journal");
    }
}
