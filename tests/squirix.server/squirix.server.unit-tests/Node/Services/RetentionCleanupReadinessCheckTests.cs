using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Unit tests for <see cref="RetentionCleanupReadinessCheck" />.
/// </summary>
[Immutable]
public sealed class RetentionCleanupReadinessCheckTests
{
    /// <summary>Ensures the readiness health check reports unhealthy when retention cleanup is degraded.</summary>
    [Fact]
    public async Task HealthReportsCleanupDegradedAsync()
    {
        var readiness = CreateReadiness(2, 5);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(true);

        var check = new RetentionCleanupReadinessCheck(readiness);
        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    private static RetentionCleanupReadiness CreateReadiness(int consecutiveWrites, int windowFailures) => new(
        new PersistenceOptions
        {
            DataDir = "unused",
            RetentionCleanupDegradedWrites = consecutiveWrites,
            RetentionCleanupDegradedWindowMinutes = 15,
            RetentionCleanupDegradedWindowFailures = windowFailures,
        });
}
