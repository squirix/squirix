using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness based on journal startup recovery completion.</summary>
[Immutable]
internal sealed class JournalRecoveryReadinessHealthCheck : IHealthCheck
{
    private readonly AsyncManualResetEvent _asyncManualResetEvent;

    internal JournalRecoveryReadinessHealthCheck(AsyncManualResetEvent asyncManualResetEvent)
    {
        ArgumentNullException.ThrowIfNull(asyncManualResetEvent);
        _asyncManualResetEvent = asyncManualResetEvent;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        _ = context;
        _ = cancellationToken;
        return Task.FromResult(
            _asyncManualResetEvent.IsSet ? HealthCheckResult.Healthy("journal recovery is complete.") : HealthCheckResult.Unhealthy("journal recovery is still in progress."));
    }
}
