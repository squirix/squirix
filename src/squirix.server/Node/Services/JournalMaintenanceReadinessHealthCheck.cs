using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness based on fatal journal maintenance failures, and degraded readiness while journal I/O is stalled.</summary>
[Immutable]
internal sealed class JournalMaintenanceReadinessHealthCheck : IHealthCheck
{
    private readonly IJournalCompactionStatus _compaction;
    private readonly IJournalCoordinator _journal;
    private readonly ISnapshotReadinessStatus _snapshot;
    private readonly JournalStallProbe? _stallProbe;
    private readonly TimeSpan _stallThreshold;
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="JournalMaintenanceReadinessHealthCheck" /> class.</summary>
    /// <param name="journal">The journal coordinator whose latched pipeline failure fails readiness.</param>
    /// <param name="compaction">The journal compaction status.</param>
    /// <param name="snapshot">The snapshot trigger status.</param>
    /// <param name="stallProbe">The probe of the journal segment I/O in progress, or <see langword="null" /> when none is wired.</param>
    /// <param name="stallThreshold">How long one segment I/O call may stay in progress before readiness is degraded.</param>
    /// <param name="timeProvider">
    /// The clock the stall duration is measured with; it must share the <see cref="System.Diagnostics.Stopwatch" /> timestamp base the probe
    /// records, as <see cref="TimeProvider.System" /> does.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="stallThreshold" /> is not positive.</exception>
    internal JournalMaintenanceReadinessHealthCheck(
        IJournalCoordinator journal,
        IJournalCompactionStatus compaction,
        ISnapshotReadinessStatus snapshot,
        JournalStallProbe? stallProbe,
        TimeSpan stallThreshold,
        TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(journal);
        ArgumentNullException.ThrowIfNull(compaction);
        ArgumentNullException.ThrowIfNull(snapshot);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(stallThreshold, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _journal = journal;
        _compaction = compaction;
        _snapshot = snapshot;
        _stallProbe = stallProbe;
        _stallThreshold = stallThreshold;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        // A latched pipeline (failed fsync/flush, segment roll, maintenance, or post-ring apply) cannot commit
        // until restart. Report the reason as type and message only: no exception, so no stack trace in the report.
        if (_journal.GetJournalThreadFailure() is { } failure)
            return Task.FromResult(HealthCheckResult.Unhealthy($"journal pipeline is latched as failed until restart: {failure.GetType().Name}: {failure.Message}"));

        if (_compaction.State is RunState.Failed)
            return Task.FromResult(HealthCheckResult.Unhealthy("journal compaction is in failed state."));

        if (_snapshot.HasFatalFailure)
            return Task.FromResult(HealthCheckResult.Unhealthy("Snapshot trigger service has a fatal failure."));

        // A stalled write or flush is not a failure: commits resume once it returns, so readiness is degraded, not unhealthy.
        var result = DescribeStalledIo() is { } stalled ? HealthCheckResult.Degraded(stalled) : HealthCheckResult.Healthy("journal maintenance is ready.");
        return Task.FromResult(result);
    }

    /// <summary>Describes the journal segment I/O call in progress for at least the stall threshold; reads the probe without locking.</summary>
    /// <returns>The description, or <see langword="null" /> when no probe is wired or no call is stalled.</returns>
    private string? DescribeStalledIo()
    {
        if (_stallProbe == null || !_stallProbe.TryReadIo(out var operation, out var startedTimestamp))
            return null;

        var elapsed = _timeProvider.GetElapsedTime(startedTimestamp);
        return elapsed < _stallThreshold ? null
            : string.Create(
                CultureInfo.InvariantCulture,
                $"journal I/O is stalled: {operation} has been in progress for {elapsed.TotalSeconds:F1} s (threshold {_stallThreshold.TotalSeconds:F1} s).");
    }
}
