using System;
using System.Threading.Tasks;
using Squirix.ProtocolModel;
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

    /// <summary>Production conformance pins the protocol model version it was verified against.</summary>
    /// <remarks>
    /// Update the pinned hash only together with a model transition or invariant change;
    /// a silent drift between the verified model and production is a conformance failure.
    /// </remarks>
    [Fact]
    public void ProtocolVersionMatchesModelManifest() => Assert.Equal("f0e518fc4db3ce67", ExploreRunner.ModelVersionHash);
}
