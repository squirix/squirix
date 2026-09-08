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
    private readonly AsyncManualResetEvent _event;

    internal JournalRecoveryReadinessHealthCheck(AsyncManualResetEvent @event)
    {
        ArgumentNullException.ThrowIfNull(@event);
        _event = @event;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var result = _event.IsSet ? HealthCheckResult.Healthy("journal recovery is complete.") : HealthCheckResult.Unhealthy("journal recovery is still in progress.");
        return Task.FromResult(result);
    }
}
