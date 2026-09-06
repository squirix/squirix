using System;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Integration checks for majority completion with a lagging follower.</summary>
public sealed class QuorumCommitTests : NodeIntegrationTestBase
{
    /// <summary>The leader and one durable follower complete RF3 without waiting for the laggard.</summary>
    [Fact]
    public async Task MajorityDoesNotWaitForLaggard()
    {
        var pipeline = new ConformanceTestKit.Pipeline(2);
        var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);
        try
        {
            var result = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), DefaultCancellationToken);
            Assert.Equal(new byte[] { 7 }, result.ToArray());
            Assert.Equal(2, pipeline.FollowerCalls);
            Assert.Equal(1UL, pipeline.CommitIndex);
            Assert.Equal(1UL, pipeline.AppliedIndex);
        }
        finally
        {
            pipeline.ReleaseLagging(ConformanceTestKit.CreateMutation(1));
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>RF=3 writes succeed on leader plus one follower while the third replica is unavailable.</summary>
    [Fact]
    public async Task RfThreeWritesWithOneReplicaUnavailable()
    {
        var pipeline = new ConformanceTestKit.Pipeline(unavailableReplica: 2);
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);

        var result = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), DefaultCancellationToken);

        Assert.Equal(new byte[] { 7 }, result.ToArray());
        Assert.Equal(2, pipeline.FollowerCalls);
        Assert.Equal(1UL, pipeline.CommitIndex);
        Assert.Equal(1UL, pipeline.AppliedIndex);
    }

    /// <summary>RF=2 writes fail when the only mirror is unavailable and no majority remains.</summary>
    [Fact]
    public async Task RfTwoFailsWriteWhenMirrorUnavailable()
    {
        var pipeline = new ConformanceTestKit.Pipeline(1);
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline, replicaCount: 2);
        try
        {
            var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
                coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(1), DefaultCancellationToken).AsTask());

            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, exception.Message, StringComparison.Ordinal);
            Assert.Equal(0UL, pipeline.CommitIndex);
        }
        finally
        {
            pipeline.ReleaseLagging(ConformanceTestKit.CreateMutation(1));
        }
    }
}
