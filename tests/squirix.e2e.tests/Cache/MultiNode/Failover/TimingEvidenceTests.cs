using System.Text.Json;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Benchmarks;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The percentiles and the limit of the failover timing evidence, which only the stress job exercises with real timings.</summary>
public sealed class TimingEvidenceTests : EndToEndTestBase
{
    /// <summary>The nearest rank percentile of thirty values takes the fifteenth for the median and the twenty-ninth for p95; one value is every percentile.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task PercentilesUseNearestRank()
    {
        var thirty = new double[30];
        for (var i = 0; i < thirty.Length; i++)
            thirty[i] = i + 1;

        _ = await Assert.That(FailoverEvidence.Percentile(thirty, 50)).IsEqualTo(15.0);
        _ = await Assert.That(FailoverEvidence.Percentile(thirty, 95)).IsEqualTo(29.0);
        _ = await Assert.That(FailoverEvidence.Percentile(thirty, 100)).IsEqualTo(30.0);
        _ = await Assert.That(FailoverEvidence.Percentile([7.0], 50)).IsEqualTo(7.0);
        _ = await Assert.That(FailoverEvidence.Percentile([7.0], 95)).IsEqualTo(7.0);
    }

    /// <summary>On a matching machine fingerprint the gate accepts a p95 at the limit and rejects one just above it.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task LimitGateSeparatesJustBelowFromJustAbove()
    {
        var machine = MachineFingerprint.Compute();

        _ = await Assert.That(FailoverEvidence.WithinLimit(machine, 4999.0)).IsTrue();
        _ = await Assert.That(FailoverEvidence.WithinLimit(machine, 5000.0)).IsTrue();
        _ = await Assert.That(FailoverEvidence.WithinLimit(machine, 5001.0)).IsFalse();
    }

    /// <summary>The evidence takes both percentile pairs from the right measure: the recovery from the stop start and the recovery since the node was down.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task EvidenceKeepsBothMeasuresApart()
    {
        var samples = new FailoverSample[30];
        for (var i = 0; i < samples.Length; i++)
            samples[i] = new FailoverSample(i + 1, "nodeA", "nodeB", 2UL, 1000.0 + i, 100.0, 900.0 + i, null, null, null, null);

        var evidence = FailoverEvidence.Build("test", new TestElectionTiming(), samples, true);

        _ = await Assert.That(evidence.FromStopStartP50Ms).IsEqualTo(1014.0);
        _ = await Assert.That(evidence.FromStopStartP95Ms).IsEqualTo(1028.0);
        _ = await Assert.That(evidence.SinceDownP50Ms).IsEqualTo(914.0);
        _ = await Assert.That(evidence.SinceDownP95Ms).IsEqualTo(928.0);
        _ = await Assert.That(evidence.Samples.Length).IsEqualTo(30);
    }

    /// <summary>The breakdown of a stop survives the evidence file; a stop without one keeps the fields empty.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task EvidenceRoundTripsStopPhases()
    {
        var withPhases = new FailoverSample(1, "nodeA", "nodeB", 2UL, 1000.0, 100.0, 900.0, null, null, null, null, 90.0, 4.0, 2.5, 97.0, 3, 88.0);
        var without = new FailoverSample(2, "nodeA", "nodeB", 2UL, 1000.0, 100.0, 900.0, null, null, null, null);
        var evidence = FailoverEvidence.Build("test", new TestElectionTiming(), [withPhases, without], true);

        var json = JsonSerializer.Serialize(evidence, FailoverEvidenceJsonContext.Default.FailoverTimingEvidence);
        var read = JsonSerializer.Deserialize(json, FailoverEvidenceJsonContext.Default.FailoverTimingEvidence);

        _ = await Assert.That(json).Contains("\"stopHostMs\": 90");
        _ = await Assert.That(json).Contains("\"stopInFlightRequests\": 3");
        _ = await Assert.That(read!.Samples[0]).IsEqualTo(withPhases);
        _ = await Assert.That(read.Samples[1]).IsEqualTo(without);
        _ = await Assert.That(read.Samples[1].StopTotalMs).IsNull();
    }
}
