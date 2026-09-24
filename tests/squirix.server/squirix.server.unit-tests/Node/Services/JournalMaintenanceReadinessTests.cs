using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
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
/// until restart, so readiness must report it with the latched reason.
/// </summary>
[Immutable]
public sealed class JournalMaintenanceReadinessTests : IsolatedStorageTestBase
{
    private const string FsyncFailureMessage = "simulated fsync failure";

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

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
        await traced.AppendPutAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
        await traced.AwaitDurabilityCommitAsync(cancellationToken);

        var result = await CreateCheck(traced).CheckHealthAsync(new HealthCheckContext(), cancellationToken);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Healthy);
    }

    /// <summary>A failed checkpoint fsync latches the pipeline; readiness is unhealthy and names the failure without a stack trace.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchedFsyncFailureIsUnhealthy(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        await using var traced = new TracingJournalCoordinatorDecorator(journal.Journal, new OpenTelemetryJournalOperationTracer());
        journal.Writer.Flush.Arm();
        await traced.AppendPutAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
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

    /// <summary>Creates the check over <paramref name="journal" />; callers pass the tracing decorator, as production resolves it from the container.</summary>
    /// <param name="journal">Journal coordinator to observe.</param>
    /// <returns>The readiness check with healthy compaction and snapshot state.</returns>
    private static JournalMaintenanceReadinessHealthCheck CreateCheck(IJournalCoordinator journal) => new(journal, new IdleCompaction(), new HealthySnapshot());

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
}
