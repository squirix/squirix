using System;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
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
}
