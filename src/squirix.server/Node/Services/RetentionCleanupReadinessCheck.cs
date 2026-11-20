using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Node.Services;

/// <summary>Reports readiness degradation when manifest retention cleanup fails persistently.</summary>
[Immutable]
internal sealed class RetentionCleanupReadinessCheck : IReadinessCheck
{
    private static readonly Task<ReadinessResult> ReadyResult = Task.FromResult(ReadinessResult.Healthy("storage retention cleanup is ready."));

    private readonly IRetentionCleanupReadinessStatus _cleanup;

    internal RetentionCleanupReadinessCheck(IRetentionCleanupReadinessStatus cleanup)
    {
        ArgumentNullException.ThrowIfNull(cleanup);
        _cleanup = cleanup;
    }

    /// <inheritdoc />
    public Task<ReadinessResult> CheckHealthAsync(CancellationToken cancellationToken) =>
        _cleanup.IsDegraded ? DegradedResultAsync() : ReadyResult;

    private Task<ReadinessResult> DegradedResultAsync() => Task.FromResult(
        ReadinessResult.Unhealthy(
            string.Create(
                CultureInfo.InvariantCulture,
                $"storage retention cleanup is degraded after {_cleanup.ConsecutiveWriteFailures} consecutive write failures and {_cleanup.RecentFailureCount} failures in the recent window.")));
}
