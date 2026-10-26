using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Tests for the admission decisions of <see cref="PressureGate" />: with a limit of 1000 bytes, critical pressure starts at 950 bytes.</summary>
[Immutable]
public sealed class PressureGateTests
{
    private const string RejectionsInstrument = "squirix_memory_rejections_total";

    /// <summary>A write is admitted when it does not grow memory, or when this node stays below critical pressure after it; nothing is counted.</summary>
    /// <param name="currentBytes">The estimated bytes the node holds.</param>
    /// <param name="growthBytes">The estimated net growth of the write.</param>
    /// <param name="magnitudeUnknown">Whether the growth of the write cannot be bounded.</param>
    [Test]
    [Arguments(0L, 100L, false)]
    [Arguments(800L, 149L, false)]
    [Arguments(960L, 0L, false)]
    [Arguments(960L, -50L, false)]
    [Arguments(900L, 0L, true)]
    [Arguments(900L, long.MaxValue, true)]
    public async Task WriteIsAdmitted(long currentBytes, long growthBytes, bool magnitudeUnknown)
    {
        using var meter = new Meter(nameof(WriteIsAdmitted));
        using var sink = new MeasurementSink(meter);
        var gate = Create(currentBytes, meter, null);

        _ = await Assert.That(gate.RejectsMemoryGrowingWrite(growthBytes, magnitudeUnknown)).IsFalse();
        gate.ThrowIfMemoryGrowingWriteRejected(growthBytes, magnitudeUnknown, AdmissionOperations.Set);

        _ = await Assert.That(sink.HasEvent(RejectionsInstrument)).IsFalse();
    }

    /// <summary>A refused write throws, counts one rejection, and the rejection metric names why; asking alone counts nothing.</summary>
    /// <param name="currentBytes">The estimated bytes the node holds.</param>
    /// <param name="growthBytes">The estimated net growth of the write.</param>
    /// <param name="magnitudeUnknown">Whether the growth of the write cannot be bounded.</param>
    /// <param name="reason">The expected rejection reason.</param>
    [Test]
    [Arguments(900L, 100L, false, PressureGate.EstimatedLimitReason)]
    [Arguments(800L, 150L, false, PressureGate.EstimatedLimitReason)]
    [Arguments(900L, long.MaxValue, false, PressureGate.EstimatedLimitReason)]
    [Arguments(960L, 10L, false, PressureGate.CriticalPressureReason)]
    [Arguments(960L, 0L, true, PressureGate.UnknownSizeReason)]
    [Arguments(960L, -50L, true, PressureGate.UnknownSizeReason)]
    [Arguments(960L, 10L, true, PressureGate.UnknownSizeReason)]
    public async Task WriteIsRejectedWithReason(long currentBytes, long growthBytes, bool magnitudeUnknown, string reason)
    {
        using var meter = new Meter(nameof(WriteIsRejectedWithReason));
        using var sink = new MeasurementSink(meter);
        var rejections = new StrongBox<int>();
        var gate = Create(currentBytes, meter, rejections);

        _ = await Assert.That(gate.RejectsMemoryGrowingWrite(growthBytes, magnitudeUnknown)).IsTrue();
        _ = await Assert.That(rejections.Value).IsEqualTo(0);
        _ = await Assert.That(sink.HasEvent(RejectionsInstrument)).IsFalse();

        _ = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(
            (gate, growthBytes, magnitudeUnknown),
            static state => state.gate.ThrowIfMemoryGrowingWriteRejected(state.growthBytes, state.magnitudeUnknown, AdmissionOperations.Set));

        _ = await Assert.That(rejections.Value).IsEqualTo(1);
        _ = await Assert.That(sink.HasEvent(RejectionsInstrument, ("reason", reason), ("operation", AdmissionOperations.Set))).IsTrue();
    }

    /// <summary>A replica pressure rejection reports its own reason and labels an unnamed operation as unknown.</summary>
    [Test]
    public async Task ReplicaRejectionLabelsUnknownOperation()
    {
        using var meter = new Meter(nameof(ReplicaRejectionLabelsUnknownOperation));
        using var sink = new MeasurementSink(meter);
        var rejections = new StrongBox<int>();
        var gate = Create(0, meter, rejections);

        _ = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(gate, static g => g.ThrowReplicaPressureRejection(string.Empty));

        _ = await Assert.That(rejections.Value).IsEqualTo(1);
        _ = await Assert.That(
                sink.HasEvent(RejectionsInstrument, ("reason", PressureGate.ReplicaPressureReason), ("operation", AdmissionOperations.Unknown)))
            .IsTrue();
    }

    /// <summary>Creates a gate over accounting that reports <paramref name="currentBytes" />.</summary>
    /// <param name="currentBytes">The estimated bytes the node holds.</param>
    /// <param name="meter">The meter of the rejection counter.</param>
    /// <param name="rejections">Counts recorded rejections; <see langword="null" /> when the test expects none, so recording one fails it.</param>
    /// <returns>The gate.</returns>
    private static PressureGate Create(long currentBytes, Meter meter, StrongBox<int>? rejections)
    {
        var accounting = new IMemoryUsageAccountingCreateExpectations();
        _ = accounting.Setups.ReadEstimatedBytes().ReturnValue(currentBytes);
        if (rejections != null)
            _ = accounting.Setups.RecordAdmissionRejection().Callback(() => rejections.Value++);

        var options = Options.Create(
            new PressureOptions { MaxEstimatedCacheBytes = 1000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        return new PressureGate(new StateEvaluator(options), accounting.Instance(), "node-a", meter);
    }
}
