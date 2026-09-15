using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Unit tests for <see cref="RetentionCleanupReadiness" /> degradation thresholds.</summary>
[Immutable]
public sealed class StorageRetentionCleanupReadinessTests
{
    /// <summary>Ensures consecutive failed writes degrade readiness once the configured threshold is reached.</summary>
    [Test]
    public async Task ConsecutiveFailedWritesDegradeReadiness()
    {
        var readiness = CreateReadiness(3, 5);

        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(true);
        _ = await Assert.That(readiness.IsDegraded).IsFalse();

        readiness.RecordWriteOutcome(true);

        _ = await Assert.That(readiness.IsDegraded).IsTrue();
        _ = await Assert.That(readiness.ConsecutiveWriteFailures).IsEqualTo(3);
    }

    /// <summary>Ensures a single failed write does not degrade readiness under the strict default thresholds.</summary>
    [Test]
    public async Task SingleFailedWriteDoesNotDegradeReadiness()
    {
        var readiness = CreateReadiness(3, 5);

        readiness.RecordWriteOutcome(true);

        _ = await Assert.That(readiness.IsDegraded).IsFalse();
        _ = await Assert.That(readiness.ConsecutiveWriteFailures).IsEqualTo(1);
        _ = await Assert.That(readiness.RecentFailureCount).IsEqualTo(1);
    }

    /// <summary>Ensures a successful write resets the consecutive failure counter.</summary>
    [Test]
    public async Task SuccessfulWriteResetsConsecutiveFailures()
    {
        var readiness = CreateReadiness(3, 5);

        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);

        _ = await Assert.That(readiness.IsDegraded).IsFalse();
        _ = await Assert.That(readiness.ConsecutiveWriteFailures).IsEqualTo(1);
    }

    /// <summary>Ensures enough failures inside the sliding window degrade readiness even when they are not consecutive writes.</summary>
    [Test]
    public async Task WindowFailureCountDegradesReadiness()
    {
        var readiness = CreateReadiness(10, 3);

        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);

        _ = await Assert.That(readiness.IsDegraded).IsTrue();
        _ = await Assert.That(readiness.RecentFailureCount).IsEqualTo(3);
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
