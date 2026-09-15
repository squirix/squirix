using System.Threading;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>End-to-end coverage for replica repair through node restart and recovery.</summary>
public sealed class ReplicaRepairE2ETests : EndToEndTestBase
{
    /// <summary>Verifies a restarted node completes recovery and serves committed entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartedNodeServesCommitted(CancellationToken cancellationToken)
    {
        await using var node = await RestartableNode.StartAsync(nameof(RestartedNodeServesCommitted), cancellationToken);
        var cache = await node.GetCacheAsync<string>("repair-recovery", cancellationToken);
        await cache.SetAsync("one", "1", cancellationToken: cancellationToken);
        await cache.SetAsync("two", "2", cancellationToken: cancellationToken);

        await node.RestartAsync(cancellationToken);
        var restarted = await node.GetCacheAsync<string>("repair-recovery", cancellationToken);

        var one = await restarted.GetValueAsync("one", cancellationToken);
        var two = await restarted.GetValueAsync("two", cancellationToken);
        _ = await Assert.That(one.Found).IsTrue().Because("Committed entry 'one' was not served after recovery.");
        _ = await Assert.That(one.Value).IsEqualTo("1");
        _ = await Assert.That(two.Found).IsTrue().Because("Committed entry 'two' was not served after recovery.");
        _ = await Assert.That(two.Value).IsEqualTo("2");
    }
}
