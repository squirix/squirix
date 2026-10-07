using System;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The catch-up reports warn once per follower and outcome, and count every session by its closed outcome name.</summary>
[Immutable]
public sealed class ReplicaCatchUpReporterTests : ServerUnitTestBase
{
    private const int CaughtUpEventId = 4021;
    private const int CompactedEventId = 4022;
    private const int InterruptedEventId = 4025;
    private const int StoppedEventId = 4023;

    /// <summary>A follower left out by the same outcome on every pass is warned about once, and again after it was admitted in between.</summary>
    [Test]
    public async Task RepeatedOutcomeIsWarnedOnce()
    {
        var log = new EventRecordingLogger();
        var reporter = new ReplicaCatchUpReporter("n1", log, null);

        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.Compacted), false);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.Compacted), false);
        reporter.Report(1, "n2", Result(ReplicaCatchUpOutcome.Compacted), false);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.CaughtUp), true);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.Compacted), false);

        _ = await Assert.That(log.Count(CompactedEventId)).IsEqualTo(3);
        _ = await Assert.That(log.Count(CaughtUpEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Find(CompactedEventId)?.Level).IsEqualTo(LogLevel.Warning);
    }

    /// <summary>An unreachable follower, and a caught-up one that was not admitted, are logged at debug level on every pass.</summary>
    [Test]
    public async Task TransientOutcomesAreDebugOnly()
    {
        var log = new EventRecordingLogger();
        var reporter = new ReplicaCatchUpReporter("n1", log, null);

        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.Unreachable), false);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.Unreachable), false);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.CaughtUp), false);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.StaleTerm), false);

        _ = await Assert.That(log.Count(InterruptedEventId)).IsEqualTo(3);
        _ = await Assert.That(log.Find(InterruptedEventId)?.Level).IsEqualTo(LogLevel.Debug);
        _ = await Assert.That(log.FindMessage(StoppedEventId)).Contains("stale_term");
    }

    /// <summary>Every session is counted under the owner node and group with its outcome name.</summary>
    [Test]
    public async Task SessionsAreCountedByOutcome()
    {
        using var meter = new Meter("Squirix");
        var counted = new ConcurrentDictionary<string, long>(StringComparer.Ordinal);
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, target) =>
        {
            if (ReferenceEquals(instrument.Meter, meter) && string.Equals(instrument.Name, "squirix_replication_catch_up_sessions_total", StringComparison.Ordinal))
                target.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            string? node = null, group = null, outcome = null;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, "node", StringComparison.Ordinal))
                    node = tag.Value as string;
                else if (string.Equals(tag.Key, "group", StringComparison.Ordinal))
                    group = tag.Value as string;
                else if (string.Equals(tag.Key, "outcome", StringComparison.Ordinal))
                    outcome = tag.Value as string;
            }

            _ = counted.AddOrUpdate($"{node}/{group}/{outcome}", static (_, added) => added, static (_, sum, added) => sum + added, value);
        });
        listener.Start();
        var reporter = new ReplicaCatchUpReporter("n1", new EventRecordingLogger(), new ReplicaCatchUpMetrics(meter));

        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.Compacted), false);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.Compacted), false);
        reporter.Report(2, "n3", Result(ReplicaCatchUpOutcome.CaughtUp), true);

        _ = await Assert.That(counted["n1/n1/compacted"]).IsEqualTo(2L);
        _ = await Assert.That(counted["n1/n1/caught_up"]).IsEqualTo(1L);
        _ = await Assert.That(counted.Count).IsEqualTo(2);
    }

    private static ReplicaCatchUpResult Result(ReplicaCatchUpOutcome outcome) => new(outcome, 0, 0, 0, 0, 1);
}
