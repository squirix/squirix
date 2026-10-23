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

    private readonly IRetentionCleanupReadinessStatus _cleanup;

    internal RetentionCleanupReadinessCheck(IRetentionCleanupReadinessStatus cleanup)
    {
        ArgumentNullException.ThrowIfNull(cleanup);
        _cleanup = cleanup;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) =>
        _cleanup.IsDegraded ? DegradedResultAsync() : ReadyResult;

    private Task<HealthCheckResult> DegradedResultAsync() => Task.FromResult(
        HealthCheckResult.Unhealthy(
            string.Create(
                CultureInfo.InvariantCulture,
                $"storage retention cleanup is degraded after {_cleanup.ConsecutiveWriteFailures} consecutive write failures and {_cleanup.RecentFailureCount} failures in the recent window.")));
}
