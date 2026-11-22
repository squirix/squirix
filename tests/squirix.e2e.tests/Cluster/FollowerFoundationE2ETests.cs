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
    /// <summary>Committed entries remain visible after a restart of the persistent node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedEntryRemainsVisibleAfterRestart(CancellationToken cancellationToken)
    {
        const string name = nameof(CommittedEntryRemainsVisibleAfterRestart);
        await using var cluster = await HostedCluster.StartSingleNodeAsync(name, persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<string>("committed-prefix", cancellationToken: cancellationToken);
        await cache.SetAsync("committed", "visible", cancellationToken: cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restartedCache = await cluster.GetCacheAsync<string>("committed-prefix", cancellationToken: cancellationToken);
        var result = await restartedCache.GetValueAsync("committed", cancellationToken);

        _ = await Assert.That(result.Found).IsTrue().Because("The committed entry was not visible after the restart.");
        _ = await Assert.That(result.Value).IsEqualTo("visible");
    }
}
