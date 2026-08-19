using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Unit tests for <see cref="RetentionCleanupReadiness" /> degradation thresholds.
/// </summary>
[Immutable]
public sealed class StorageRetentionCleanupReadinessTests
{
    /// <summary>Ensures consecutive failed writes degrade readiness once the configured threshold is reached.</summary>
    [Fact]
    public void ConsecutiveFailedWritesDegradeReadiness()
    {
        var readiness = CreateReadiness(3, 5);

        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(true);
        Assert.False(readiness.IsDegraded);

        readiness.RecordWriteOutcome(true);

        Assert.True(readiness.IsDegraded);
        Assert.Equal(3, readiness.ConsecutiveWriteFailures);
    }

    /// <summary>Ensures the readiness health check reports unhealthy when retention cleanup is degraded.</summary>
    [Fact]
    public async Task HealthCheckReportsRetentionCleanupDegradedAsync()
    {
        var readiness = CreateReadiness(2, 5);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(true);

        var check = new RetentionCleanupReadinessCheck(readiness);
        var result = await check.CheckHealthAsync(new HealthCheckContext(), CancellationToken.None);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    /// <summary>Ensures a single failed write does not degrade readiness under the strict default thresholds.</summary>
    [Fact]
    public void SingleFailedWriteDoesNotDegradeReadiness()
    {
        var readiness = CreateReadiness(3, 5);

        readiness.RecordWriteOutcome(true);

        Assert.False(readiness.IsDegraded);
        Assert.Equal(1, readiness.ConsecutiveWriteFailures);
        Assert.Equal(1, readiness.RecentFailureCount);
    }

    /// <summary>Ensures a successful write resets the consecutive failure counter.</summary>
    [Fact]
    public void SuccessfulWriteResetsConsecutiveFailures()
    {
        var readiness = CreateReadiness(3, 5);

        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);

        Assert.False(readiness.IsDegraded);
        Assert.Equal(1, readiness.ConsecutiveWriteFailures);
    }

    /// <summary>Ensures enough failures inside the sliding window degrade readiness even when they are not consecutive writes.</summary>
    [Fact]
    public void WindowFailureCountDegradesReadiness()
    {
        var readiness = CreateReadiness(10, 3);

        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);

        Assert.True(readiness.IsDegraded);
        Assert.Equal(3, readiness.RecentFailureCount);
    }

    private static RetentionCleanupReadiness CreateReadiness(int consecutiveWrites, int windowFailures) => new(
        new PersistenceOptions
        {
            DataDir = "unused",
            RetentionCleanupDegradedConsecutiveWrites = consecutiveWrites,
            RetentionCleanupDegradedWindowMinutes = 15,
            RetentionCleanupDegradedWindowFailures = windowFailures,
        });
}
