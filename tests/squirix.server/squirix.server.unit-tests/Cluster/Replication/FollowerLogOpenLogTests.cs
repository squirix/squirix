using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A group log that opens with a copy that is not complete says so once, naming the group and the state.</summary>
[Immutable]
public sealed class FollowerLogOpenLogTests : ServerUnitTestBase
{
    /// <summary>The event id of a log opened with a copy that is not complete.</summary>
    private const int OpenedNotCompleteEventId = 4062;

    private const string Group = "n1";

    /// <summary>A log on a fresh disk logs one line naming the group and the unseeded state, and a reopened complete copy logs none.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshCopyLogsUnseededOnOpen(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-open-log-fresh");
        var fresh = new EventRecordingLogger();
        await using (var log = Create(dir, fresh))
            await log.OpenAsync(cancellationToken);

        var reopened = new EventRecordingLogger();
        await using (var log = Create(dir, reopened))
        {
            await log.OpenAsync(cancellationToken);
            await log.CompleteBootstrapAsync(cancellationToken);
        }

        var complete = new EventRecordingLogger();
        await using (var log = Create(dir, complete))
            await log.OpenAsync(cancellationToken);

        _ = await Assert.That(fresh.Count(OpenedNotCompleteEventId)).IsEqualTo(1);
        _ = await Assert.That(fresh.Find(OpenedNotCompleteEventId)?.Level).IsEqualTo(LogLevel.Information);
        _ = await Assert.That(fresh.FindMessage(OpenedNotCompleteEventId)).Contains("Replica group n1 log opened with a copy that is unseeded");
        _ = await Assert.That(reopened.Count(OpenedNotCompleteEventId)).IsEqualTo(1);
        _ = await Assert.That(complete.Count(OpenedNotCompleteEventId)).IsEqualTo(0);
    }

    private static FollowerLog Create(string dir, EventRecordingLogger logger) =>
        new(dir, Group, GroupComposition.Create(Group), logger, TimeProvider.System);
}
