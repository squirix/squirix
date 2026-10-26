using System.Diagnostics.Metrics;
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

    /// <summary>A write is admitted when it does not grow memory, or when this node stays below critical pressure after it.</summary>
    /// <param name="currentBytes">The estimated bytes the node holds.</param>
    /// <param name="growthBytes">The estimated net growth of the write.</param>
    /// <param name="magnitudeUnknown">Whether the growth of the write cannot be bounded.</param>
    [Test]
    [Arguments(0L, 100L, false)]
    [Arguments(800L, 149L, false)]
    [Arguments(960L, 0L, false)]
    [Arguments(960L, -50L, false)]
    [Arguments(900L, 0L, true)]
    public async Task WriteIsAdmitted(long currentBytes, long growthBytes, bool magnitudeUnknown)
    {
        using var meter = new Meter(nameof(WriteIsAdmitted));
        var gate = Create(currentBytes, meter);

        _ = await Assert.That(gate.RejectsMemoryGrowingWrite(growthBytes, magnitudeUnknown)).IsFalse();
        gate.ThrowIfMemoryGrowingWriteRejected(growthBytes, magnitudeUnknown, AdmissionOperations.Set);
    }

    /// <summary>A refused write throws, and the rejection metric names why it was refused.</summary>
    /// <param name="currentBytes">The estimated bytes the node holds.</param>
    /// <param name="growthBytes">The estimated net growth of the write.</param>
    /// <param name="magnitudeUnknown">Whether the growth of the write cannot be bounded.</param>
    /// <param name="reason">The expected rejection reason.</param>
    [Test]
    [Arguments(900L, 100L, false, PressureGate.EstimatedLimitReason)]
    [Arguments(900L, long.MaxValue, false, PressureGate.EstimatedLimitReason)]
    [Arguments(960L, 10L, false, PressureGate.CriticalPressureReason)]
    [Arguments(960L, 0L, true, PressureGate.UnknownSizeReason)]
    [Arguments(960L, 10L, true, PressureGate.UnknownSizeReason)]
    public async Task WriteIsRejectedWithReason(long currentBytes, long growthBytes, bool magnitudeUnknown, string reason)
    {
        using var meter = new Meter(nameof(WriteIsRejectedWithReason));
        using var sink = new MeasurementSink(meter);
        var gate = Create(currentBytes, meter);

        _ = await Assert.That(gate.RejectsMemoryGrowingWrite(growthBytes, magnitudeUnknown)).IsTrue();
        _ = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(
            (gate, growthBytes, magnitudeUnknown),
            static state => state.gate.ThrowIfMemoryGrowingWriteRejected(state.growthBytes, state.magnitudeUnknown, AdmissionOperations.Set));

        _ = await Assert.That(sink.HasEvent(RejectionsInstrument, ("reason", reason), ("operation", AdmissionOperations.Set))).IsTrue();
    }

    /// <summary>A replica pressure rejection reports its own reason and labels an unnamed operation as unknown.</summary>
    [Test]
    public async Task ReplicaRejectionLabelsUnknownOperation()
    {
        using var meter = new Meter(nameof(ReplicaRejectionLabelsUnknownOperation));
        using var sink = new MeasurementSink(meter);
        var gate = Create(0, meter);

        _ = NodeExceptionAssert.For<ResourceExhaustedException>().Throws(gate, static g => g.ThrowReplicaPressureRejection(string.Empty));

        _ = await Assert.That(
                sink.HasEvent(RejectionsInstrument, ("reason", PressureGate.ReplicaPressureReason), ("operation", AdmissionOperations.Unknown)))
            .IsTrue();
    }

    private static PressureGate Create(long currentBytes, Meter meter)
    {
        var accounting = new IMemoryUsageAccountingCreateExpectations();
        _ = accounting.Setups.ReadEstimatedBytes().ReturnValue(currentBytes);
        _ = accounting.Setups.RecordAdmissionRejection();
        var options = Options.Create(
            new PressureOptions { MaxEstimatedCacheBytes = 1000, HighPressureThresholdPercent = 80, CriticalPressureThresholdPercent = 95 });
        return new PressureGate(new StateEvaluator(options), accounting.Instance(), "node-a", meter);
    }
}
