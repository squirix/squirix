using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Unit tests for <see cref="JournalMaintenanceReadinessHealthCheck" />: a journal pipeline latched as failed cannot commit
/// until restart, so readiness must report it with the latched reason; a journal I/O call stalled past the threshold degrades
/// readiness until it returns.
/// </summary>
[Immutable]
public sealed class JournalMaintenanceReadinessTests : IsolatedStorageTestBase
{
    private const string FsyncFailureMessage = "simulated fsync failure";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    private static readonly TimeSpan Threshold = new PersistenceOptions().JournalStallDegradedThreshold;

    /// <summary>A pipeline latched by a caller (memory apply failure after ring entry) reports the latched reason, not a flush-loop message.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedPipelineReportsReason(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        journal.Journal.FailJournalPipeline(new InvalidOperationException("memory apply failed"));

        var result = await CreateCheck(traced).CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("memory apply failed", StringComparison.Ordinal);
    }

    /// <summary>A healthy journal that commits durably keeps readiness healthy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HealthyJournalReportsHealthy(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        await traced.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await traced.AwaitDurabilityCommitAsync(cancellationToken);

        var result = await CreateCheck(traced).CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>A latched pipeline stays unhealthy while a flush is also stalled past the threshold: the latch takes precedence over degraded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchedFailureStaysUnhealthy(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        journal.Writer.Flush.Arm();
        await traced.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        var commit = traced.AwaitDurabilityCommitAsync(cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        journal.Journal.FailJournalPipeline(new InvalidOperationException("memory apply failed"));

        var result = await CreateCheck(traced, journal.Journal.StallProbe, new ShiftedClock(Threshold)).CheckHealthAsync(new HealthCheckContext(), cancellationToken);
        journal.Writer.Flush.ReleaseWithFailure(new IOException(FsyncFailureMessage));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(commit.WaitAsync(Bound, TimeProvider.System, cancellationToken));

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains("memory apply failed", StringComparison.Ordinal);
    }

    /// <summary>A failed checkpoint fsync latches the pipeline; readiness is unhealthy and names the failure without a stack trace.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchedFsyncFailureIsUnhealthy(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        journal.Writer.Flush.Arm();
        await traced.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        var commit = traced.AwaitDurabilityCommitAsync(cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        journal.Writer.Flush.ReleaseWithFailure(new IOException(FsyncFailureMessage));
        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(commit.WaitAsync(Bound, TimeProvider.System, cancellationToken));

        // The checkpoint waiter is faulted before the journal thread latches the failure; its exit orders the latch first.
        _ = await Assert.That(await journal.Journal.DurabilityPipeline.TryJoinJournalThreadAsync(Bound)).IsTrue();
        var result = await CreateCheck(traced).CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(result.Description).Contains(nameof(IOException), StringComparison.Ordinal);
        _ = await Assert.That(result.Description).Contains(FsyncFailureMessage, StringComparison.Ordinal);
        _ = await Assert.That(result.Description).DoesNotContain(" at ", StringComparison.Ordinal);
        _ = await Assert.That(result.Exception).IsNull();
    }

    /// <summary>Without a stall probe (a journal that does not record its segment I/O) a stalled flush never degrades readiness.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoProbeStaysHealthy(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        journal.Writer.Flush.Arm();
        await traced.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        var commit = traced.AwaitDurabilityCommitAsync(cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        var result = await CreateCheck(traced, null, new ShiftedClock(Threshold)).CheckHealthAsync(new HealthCheckContext(), cancellationToken);
        journal.Writer.Flush.Release();
        await commit.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>A flush in progress for less than the threshold keeps readiness healthy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShortStallStaysHealthy(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        journal.Writer.Flush.Arm();
        await traced.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        var commit = traced.AwaitDurabilityCommitAsync(cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        var result = await CreateCheck(traced, journal.Journal.StallProbe, new ShiftedClock(Threshold / 2)).CheckHealthAsync(new HealthCheckContext(), cancellationToken);
        journal.Writer.Flush.Release();
        await commit.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>A flush in progress for the threshold degrades readiness and names the operation; readiness recovers once the flush returns.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StalledFlushIsDegraded(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        var check = CreateCheck(traced, journal.Journal.StallProbe, new ShiftedClock(Threshold));
        journal.Writer.Flush.Arm();
        await traced.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        var commit = traced.AwaitDurabilityCommitAsync(cancellationToken).AsTask();
        await journal.Writer.Flush.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        var stalled = await check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);
        journal.Writer.Flush.Release();
        await commit.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        var recovered = await check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(stalled.Status).IsEqualTo(HealthStatus.Degraded);
        _ = await Assert.That(stalled.Description).Contains($": {nameof(IJournalSegmentWriter.FlushToDisk)} ", StringComparison.Ordinal);
        _ = await Assert.That(recovered.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>A segment write in progress for the threshold degrades readiness and names the operation; readiness recovers once it returns.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StalledWriteIsDegraded(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        var check = CreateCheck(traced, journal.Journal.StallProbe, new ShiftedClock(Threshold));
        journal.Writer.Write.Arm();
        var append = traced.AppendAdmittedUnderGateAsync(
            CacheKey.Default("a"),
            static (appender, key, ct) => appender.AppendPutAndAwaitDurabilityAsync(key, JournalEntryPayloadKit.EncodePut("a"), ct),
            cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        var stalled = await check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);
        journal.Writer.Write.Release();

        // The durable append returns only after its write and fsync returned, so no segment I/O is left in progress.
        await append.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        var recovered = await check.CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(stalled.Status).IsEqualTo(HealthStatus.Degraded);
        _ = await Assert.That(stalled.Description).Contains($": {nameof(IJournalSegmentWriter.Write)} ", StringComparison.Ordinal);
        _ = await Assert.That(recovered.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>Creates the check over <paramref name="journal" />; callers pass the tracing decorator, as production resolves it from the container.</summary>
    /// <param name="journal">Journal coordinator to observe.</param>
    /// <returns>The readiness check with healthy compaction and snapshot state and no stall probe.</returns>
    private static JournalMaintenanceReadinessHealthCheck CreateCheck(IJournalCoordinator journal) => CreateCheck(journal, null, TimeProvider.System);

    /// <summary>Creates the check over <paramref name="journal" /> with a stall probe read through <paramref name="clock" />.</summary>
    /// <param name="journal">Journal coordinator to observe.</param>
    /// <param name="stallProbe">Segment I/O stall probe, or <see langword="null" /> for none.</param>
    /// <param name="clock">Clock the stall duration is measured with.</param>
    /// <returns>The readiness check with healthy compaction and snapshot state and the default stall threshold.</returns>
    private static JournalMaintenanceReadinessHealthCheck CreateCheck(IJournalCoordinator journal, JournalStallProbe? stallProbe, TimeProvider clock) =>
        new(journal, new IdleCompaction(), new HealthySnapshot(), stallProbe, new PersistenceOptions().JournalStallDegradedThreshold, clock);

    [Immutable]
    private sealed class IdleCompaction : IJournalCompactionStatus
    {
        public bool IsInFlight => false;

        public DateTime LastRunUtc => DateTime.MinValue;

        public RunState State => RunState.Idle;
    }

    [Immutable]
    private sealed class HealthySnapshot : ISnapshotReadinessStatus
    {
        public bool HasFatalFailure => false;
    }

    /// <summary>The system clock shifted forward by a fixed amount, so an operation in progress looks that much older than it is.</summary>
    [Immutable]
    private sealed class ShiftedClock : TimeProvider
    {
        private readonly long _shift;

        internal ShiftedClock(TimeSpan shift)
        {
            _shift = Convert.ToInt64(shift.TotalSeconds * Stopwatch.Frequency);
        }

        public override long GetTimestamp() => base.GetTimestamp() + _shift;
    }
}
