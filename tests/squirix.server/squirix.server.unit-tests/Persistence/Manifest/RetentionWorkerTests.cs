using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Covers RetentionWorker schedule/rearm and invalid DataDir cleanup paths.</summary>
public sealed class RetentionWorkerTests : ServerUnitTestBase
{
    /// <summary>Invalid DataDir causes cleanup failure reporting without crashing the worker.</summary>
    [Fact]
    public async Task ScheduleRetentionCleanupWithInvalidDataDirRecordsFailure()
    {
        var readiness = new RecordingReadiness();
        var metrics = new RecordingFailureMetrics();
        var context = new RetentionContext(new RetentionSettings("..", 1, 1, "man-*.bmqx"), null, null, static _ => 1, metrics);
        var worker = new RetentionWorker(context, readiness);

        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 2 });

        await WaitUntilAsync(static r => r.Outcomes.Count > 0, readiness, DefaultCancellationToken);

        Assert.Contains(true, readiness.Outcomes);
        Assert.True(metrics.Failures > 0);
    }

    private static async Task WaitUntilAsync(Func<RecordingReadiness, bool> condition, RecordingReadiness readiness, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition(readiness))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (DateTime.UtcNow >= deadline)
                throw new TimeoutException("Timed out waiting for retention worker outcome.");

            await Task.Delay(10, cancellationToken);
        }
    }

    private sealed class RecordingFailureMetrics : IManifestRetentionFailureMetrics
    {
        internal int Failures { get; private set; }

        public void RecordDeleteFailure(string artifactKind, string outcome)
        {
            _ = artifactKind;
            _ = outcome;
            Failures++;
        }
    }

    private sealed class RecordingReadiness : IRetentionCleanupReadinessStatus
    {
        public int ConsecutiveWriteFailures => Outcomes.Count;

        public bool IsDegraded => false;

        public DateTime? LastFailureUtc => null;

        public int RecentFailureCount => Outcomes.Count;

        internal List<bool> Outcomes { get; } = [];

        public void RecordWriteOutcome(bool hadFailure) => Outcomes.Add(hadFailure);
    }
}
