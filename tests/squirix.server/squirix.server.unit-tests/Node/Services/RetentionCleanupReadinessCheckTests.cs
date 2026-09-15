using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for <see cref="RetentionCleanupReadinessCheck" />.</summary>
[Immutable]
public sealed class RetentionCleanupReadinessCheckTests
{
    /// <summary>Ensures the readiness health check reports unhealthy when retention cleanup is degraded.</summary>
    [Test]
    public async Task HealthReportsCleanupDegradedAsync()
    {
        var readiness = CreateReadiness(2, 5);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(true);

        var check = new RetentionCleanupReadinessCheck(readiness);
        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        _ = await Assert.That(result.Status).IsEqualTo(HealthStatus.Unhealthy);
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
