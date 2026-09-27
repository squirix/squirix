using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
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
        var outcomes = new ConcurrentQueue<bool>();
        var readiness = new IRetentionCleanupReadinessStatusCreateExpectations();
        _ = readiness.Setups.RecordWriteOutcome(Arg.Any<bool>()).Callback(outcomes.Enqueue);
        var failures = 0;
        var metrics = new IManifestRetentionFailureMetricsCreateExpectations();
        _ = metrics.Setups.RecordDeleteFailure(Arg.Any<string>(), Arg.Any<string>()).Callback((_, _) => Interlocked.Increment(ref failures));
        var context = new RetentionContext(new RetentionSettings("..", 1, 1, "man-*.bmqx"), null, null, static _ => 1, metrics.Instance());
        var worker = new RetentionWorker(context, readiness.Instance());

        worker.ScheduleRetentionCleanup(new State { CurrentJournal = 2 });

        await outcomes.WaitUntilAsync(static o => !o.IsEmpty, cancellationToken);

        _ = await Assert.That(outcomes).Contains(true);
        _ = await Assert.That(Volatile.Read(ref failures) > 0).IsTrue();
    }
}
