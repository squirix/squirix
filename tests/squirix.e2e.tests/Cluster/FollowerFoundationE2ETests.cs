using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>Closed follower-foundation persistence and activation safety scenarios.</summary>
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

    /// <summary>A node restart restores committed cache entries and their journal tail records.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartRestoresEntriesAndTail(CancellationToken cancellationToken)
    {
        const string name = nameof(RestartRestoresEntriesAndTail);
        await using var cluster = await HostedCluster.StartSingleNodeAsync(name, persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<string>("snapshot-journal", cancellationToken: cancellationToken);
        await cache.SetAsync("committed", "baseline", cancellationToken: cancellationToken);
        await cache.SetAsync("tail", "journal", cancellationToken: cancellationToken);

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var restartedCache = await cluster.GetCacheAsync<string>("snapshot-journal", cancellationToken: cancellationToken);
        var committed = await restartedCache.GetValueAsync("committed", cancellationToken);
        var tail = await restartedCache.GetValueAsync("tail", cancellationToken);

        _ = await Assert.That(committed.Found).IsTrue().Because("The committed baseline was not restored after the restart.");
        _ = await Assert.That(committed.Value).IsEqualTo("baseline");
        _ = await Assert.That(tail.Found).IsTrue().Because("The journal tail was not restored after the restart.");
        _ = await Assert.That(tail.Value).IsEqualTo("journal");
    }
}
