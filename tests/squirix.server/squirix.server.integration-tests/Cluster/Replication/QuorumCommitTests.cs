using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Integration checks for majority completion with a lagging follower.</summary>
public sealed class QuorumCommitTests : NodeIntegrationTestBase
{
    /// <summary>The leader and one durable follower complete RF3 without waiting for the laggard.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MajorityDoesNotWaitForLaggard(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline(2);
        var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);
        try
        {
            var result = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);
            await SequenceAssert.Equal<byte>([7], result.ToArray());
            _ = await Assert.That(pipeline.FollowerCalls).IsEqualTo(2);
            _ = await Assert.That(pipeline.CommitIndex).IsEqualTo(1UL);
            _ = await Assert.That(pipeline.AppliedIndex).IsEqualTo(1UL);
        }
        finally
        {
            pipeline.ReleaseLagging(ConformanceTestKit.CreateMutation(1));
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>RF=3 writes succeed on leader plus one follower while the third replica is unavailable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfThreeWritesWithOneReplicaUnavailable(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline(unavailableReplica: 2);
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);

        var result = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), cancellationToken);

        await SequenceAssert.Equal<byte>([7], result.ToArray());
        _ = await Assert.That(pipeline.FollowerCalls).IsEqualTo(2);
        _ = await Assert.That(pipeline.CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That(pipeline.AppliedIndex).IsEqualTo(1UL);
    }

    /// <summary>RF=2 writes fail when the only mirror is unavailable and no majority remains.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoFailsWriteWhenMirrorUnavailable(CancellationToken cancellationToken)
    {
        var pipeline = new ConformanceTestKit.Pipeline(1);
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, replicaCount: 2);
        try
        {
            var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
                coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(1), cancellationToken).AsTask());

            _ = await Assert.That(exception.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(pipeline.CommitIndex).IsEqualTo(0UL);
        }
        finally
        {
            pipeline.ReleaseLagging(ConformanceTestKit.CreateMutation(1));
        }
    }
}
