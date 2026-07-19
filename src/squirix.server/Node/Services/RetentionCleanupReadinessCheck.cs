using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness degradation when manifest retention cleanup fails persistently.</summary>
internal sealed class RetentionCleanupReadinessCheck : IHealthCheck
{
    private readonly IRetentionCleanupReadinessStatus _retentionCleanup;

    internal RetentionCleanupReadinessCheck(IRetentionCleanupReadinessStatus retentionCleanup)
    {
        _retentionCleanup = retentionCleanup ?? throw new ArgumentNullException(nameof(retentionCleanup));
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        _ = context;
        _ = cancellationToken;

        if (!_retentionCleanup.IsDegraded)
            return Task.FromResult(HealthCheckResult.Healthy("storage retention cleanup is ready."));

        return Task.FromResult(
            HealthCheckResult.Unhealthy(
                $"storage retention cleanup is degraded after {_retentionCleanup.ConsecutiveWriteFailures.ToString(CultureInfo.InvariantCulture)} consecutive write failures and {_retentionCleanup.RecentFailureCount.ToString(CultureInfo.InvariantCulture)} failures in the recent window."));
    }
}
