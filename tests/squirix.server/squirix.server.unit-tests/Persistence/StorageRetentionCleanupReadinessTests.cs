using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
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
        var readiness = CreateReadiness(3, 5, TimeProvider.System);

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
        var readiness = CreateReadiness(3, 5, TimeProvider.System);

        readiness.RecordWriteOutcome(true);

        _ = await Assert.That(readiness.IsDegraded).IsFalse();
        _ = await Assert.That(readiness.ConsecutiveWriteFailures).IsEqualTo(1);
        _ = await Assert.That(readiness.RecentFailureCount).IsEqualTo(1);
    }

    /// <summary>Window failures stop degrading readiness once the server clock passes the window, with no further cleanup outcome.</summary>
    [Test]
    public async Task WindowFailuresExpireOnServerClock()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var readiness = CreateReadiness(10, 5, clock);
        for (var i = 0; i < 5; i++)
        {
            readiness.RecordWriteOutcome(true);
            readiness.RecordWriteOutcome(false);
        }

        var degraded = readiness.IsDegraded;
        clock.Advance(TimeSpan.FromMinutes(15) + TimeSpan.FromSeconds(1));

        _ = await Assert.That(degraded).IsTrue();
        _ = await Assert.That(readiness.IsDegraded).IsFalse();
        _ = await Assert.That(readiness.RecentFailureCount).IsEqualTo(0);
    }

    /// <summary>Ensures a successful write resets the consecutive failure counter.</summary>
    [Test]
    public async Task SuccessfulWriteResetsConsecutiveFailures()
    {
        var readiness = CreateReadiness(3, 5, TimeProvider.System);

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
        var readiness = CreateReadiness(10, 3, TimeProvider.System);

        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);
        readiness.RecordWriteOutcome(false);
        readiness.RecordWriteOutcome(true);

        _ = await Assert.That(readiness.IsDegraded).IsTrue();
        _ = await Assert.That(readiness.RecentFailureCount).IsEqualTo(3);
    }

    /// <summary>Rejects a non-positive consecutive-write threshold, which would report degraded readiness before any failure.</summary>
    /// <param name="value">The configured threshold.</param>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task RejectsNonPositiveWrites(int value)
    {
        var options = new PersistenceOptions { DataDir = "unused", RetentionCleanupDegradedWrites = value };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static o => o.Validate());

        _ = await Assert.That(ex.Message).Contains(nameof(PersistenceOptions.RetentionCleanupDegradedWrites));
    }

    /// <summary>Rejects a non-positive window failure threshold, which would report degraded readiness before any failure.</summary>
    /// <param name="value">The configured threshold.</param>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task RejectsNonPositiveWindowFailures(int value)
    {
        var options = new PersistenceOptions { DataDir = "unused", RetentionCleanupDegradedWindowFailures = value };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static o => o.Validate());

        _ = await Assert.That(ex.Message).Contains(nameof(PersistenceOptions.RetentionCleanupDegradedWindowFailures));
    }

    /// <summary>Rejects a non-positive window length, which would silently disable the windowed check.</summary>
    /// <param name="value">The configured window in minutes.</param>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task RejectsNonPositiveWindowMinutes(int value)
    {
        var options = new PersistenceOptions { DataDir = "unused", RetentionCleanupDegradedWindowMinutes = value };

        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static o => o.Validate());

        _ = await Assert.That(ex.Message).Contains(nameof(PersistenceOptions.RetentionCleanupDegradedWindowMinutes));
    }

    /// <summary>Ensures the minimum valid thresholds keep readiness healthy until a threshold is actually reached.</summary>
    [Test]
    public async Task MinimumThresholdsHealthyUntilReached()
    {
        var options = new PersistenceOptions
        {
            DataDir = "unused",
            RetentionCleanupDegradedWrites = 1,
            RetentionCleanupDegradedWindowFailures = 1,
            RetentionCleanupDegradedWindowMinutes = 1,
        };
        options.Validate();
        var readiness = new RetentionCleanupReadiness(options, TimeProvider.System);

        readiness.RecordWriteOutcome(false);
        var healthy = !readiness.IsDegraded;
        readiness.RecordWriteOutcome(true);

        _ = await Assert.That(healthy).IsTrue();
        _ = await Assert.That(readiness.IsDegraded).IsTrue();
    }

    private static RetentionCleanupReadiness CreateReadiness(int consecutiveWrites, int windowFailures, TimeProvider clock) => new(
        new PersistenceOptions
        {
            DataDir = "unused",
            RetentionCleanupDegradedWrites = consecutiveWrites,
            RetentionCleanupDegradedWindowMinutes = 15,
            RetentionCleanupDegradedWindowFailures = windowFailures,
        },
        clock);
}
