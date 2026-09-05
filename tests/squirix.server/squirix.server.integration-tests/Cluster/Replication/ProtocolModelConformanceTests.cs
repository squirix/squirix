using System;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Checks production durable boundaries against the protocol transition system.</summary>
public sealed class ProtocolModelConformanceTests : NodeIntegrationTestBase
{
    /// <summary>A production commit trace follows a path accepted by the protocol model.</summary>
    [Fact]
    public async Task ProductionCommitTraceMatchesModel()
    {
        var pipeline = new ConformanceTestKit.Pipeline();
        await using var coordinator = ConformanceTestKit.CreateCoordinator(pipeline);

        _ = await coordinator.CommitAsync(ConformanceTestKit.CreateMutation(1), TimeSpan.FromSeconds(2), DefaultCancellationToken);

        Assert.Equal(
        [
            new ConformanceTestKit.TracePoint(1, 1, 0, 0),
            new ConformanceTestKit.TracePoint(1, 1, 1, 0),
            new ConformanceTestKit.TracePoint(1, 1, 1, 1),
        ],
        pipeline.Trace);
        ConformanceTestKit.AssertModelAccepted(pipeline.Trace);
    }
}
