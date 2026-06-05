using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Reports readiness based on journal startup recovery completion.
/// </summary>
internal sealed class JournalRecoveryReadinessHealthCheck : IHealthCheck
{
    private readonly JournalStartupGate _journalStartupGate;

    public JournalRecoveryReadinessHealthCheck(JournalStartupGate journalStartupGate)
    {
        _journalStartupGate = journalStartupGate ?? throw new ArgumentNullException(nameof(journalStartupGate));
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        _ = context;
        _ = cancellationToken;
        return Task.FromResult(
            _journalStartupGate.IsReady ? HealthCheckResult.Healthy("journal recovery is complete.") : HealthCheckResult.Unhealthy("journal recovery is still in progress."));
    }
}
