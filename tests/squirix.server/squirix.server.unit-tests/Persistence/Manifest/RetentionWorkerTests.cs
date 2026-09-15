using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Covers RetentionWorker schedule/rearm and invalid DataDir cleanup paths.</summary>
[Immutable]
public sealed class RetentionWorkerTests : ServerUnitTestBase
{
    /// <summary>Invalid DataDir causes cleanup failure reporting without crashing the worker.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CleanupWithBadDirRecordsFailure(CancellationToken cancellationToken)
    {
        var readiness = new RecordingReadiness();
        var metrics = new RecordingFailureMetrics();
        var context = new RetentionContext(new RetentionSettings("..", 1, 1, "man-*.bmqx"), null, null, static _ => 1, metrics);
        var worker = new RetentionWorker(context, readiness);

        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 2 });

        await readiness.WaitUntilAsync(static r => r.Outcomes.Count > 0, cancellationToken);

        _ = await Assert.That(readiness.Outcomes).Contains(true);
        _ = await Assert.That(metrics.Failures > 0).IsTrue();
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

    [Immutable]
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
