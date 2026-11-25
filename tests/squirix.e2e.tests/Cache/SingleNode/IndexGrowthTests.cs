using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Checks acknowledged writes and deletes from two SDK connections while the server index grows.</summary>
[ParallelLimiter<ClusterStartupLimit>]
public sealed class IndexGrowthTests
{
    /// <summary>A second client observes its mutations and a stable key across repeated doublings.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MutationsSurviveConcurrentGrowth(CancellationToken cancellationToken)
    {
        const int batchSize = 32;
        const string cacheName = "growing";
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(MutationsSurviveConcurrentGrowth), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var writer = await cluster.GetCacheAsync<string>(cacheName, cancellationToken: cancellationToken);
        var second = await cluster.ConnectClientAsync(cancellationToken: cancellationToken);
        var observer = await second.GetCacheAsync<string>(cacheName, cancellationToken);
        await writer.SetAsync("stable", "original", cancellationToken: cancellationToken);
        var pending = new Task[batchSize + 1];
        for (var batch = 0; batch < 256; batch++)
        {
            for (var slot = 0; slot < batchSize; slot++)
            {
                var key = ((batch * batchSize) + slot).ToString(CultureInfo.InvariantCulture);
                pending[slot] = writer.SetAsync(key, key, cancellationToken: cancellationToken);
            }

            var expected = batch.ToString(CultureInfo.InvariantCulture);
            pending[batchSize] = ProbeAsync(expected);
            await Task.WhenAll(pending);
            _ = await Assert.That((await observer.GetValueAsync(expected, cancellationToken)).Value).IsEqualTo(expected);
        }

        async Task ProbeAsync(string value)
        {
            _ = await Assert.That((await observer.GetValueAsync("stable", cancellationToken)).Value).IsEqualTo("original");
            await observer.SetAsync("changing", value, cancellationToken: cancellationToken);
            _ = await Assert.That((await observer.GetValueAsync("changing", cancellationToken)).Value).IsEqualTo(value);
            _ = await Assert.That(await observer.RemoveAsync("changing", cancellationToken)).IsTrue();
            _ = await Assert.That((await observer.GetValueAsync("changing", cancellationToken)).Found).IsFalse();
        }
    }
}
