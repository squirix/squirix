using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness degradation when manifest retention cleanup fails persistently.</summary>
[Immutable]
internal sealed class RetentionCleanupReadinessCheck : IHealthCheck
{
    private static readonly Task<HealthCheckResult> ReadyResult = Task.FromResult(HealthCheckResult.Healthy("storage retention cleanup is ready."));

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
            return ReadyResult;

        return Task.FromResult(
            HealthCheckResult.Unhealthy(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"storage retention cleanup is degraded after {_retentionCleanup.ConsecutiveWriteFailures} consecutive write failures and {_retentionCleanup.RecentFailureCount} failures in the recent window.")));
    }
}
