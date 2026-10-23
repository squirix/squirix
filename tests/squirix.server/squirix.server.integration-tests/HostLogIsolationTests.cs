using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Two hosts in one process each report their journal startup repairs and stall warnings to their own logger and never to the other host's.</summary>
public sealed class HostLogIsolationTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const int JournalWaitCanceledWhileStalledEventId = 1014;
    private const int TornTailTruncatedEventId = 1018;

    /// <summary>
    /// A repair found while one host opens its journal is logged by that host only, under the journal host category. The hosts start
    /// sequentially, so this is a smoke check; isolation is proven by <see cref="StallWarningReachesOnlyOwnLogger" />.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepairsReachOnlyOwnHostLogger(CancellationToken cancellationToken)
    {
        var pathA = await PrepareTornSegmentAsync("node-a", "log-isolation-a", cancellationToken);
        var pathB = await PrepareTornSegmentAsync("node-b", "log-isolation-b", cancellationToken);

        using var recorderA = new RecordingLoggerProvider();
        using var recorderB = new RecordingLoggerProvider();
        await using var clusterA = await StartClusterAsync("node-a", Options("log-isolation-a", false, recorderA), cancellationToken);
        await using var clusterB = await StartClusterAsync("node-b", Options("log-isolation-b", false, recorderB), cancellationToken);

        await AssertRepairLoggedOnlyForAsync(recorderA, pathA, pathB);
        await AssertRepairLoggedOnlyForAsync(recorderB, pathB, pathA);
    }

    /// <summary>A stall warning raised by one host journal is logged by that host only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The host journal does not expose its stall probe.</exception>
    [Test]
    public async Task StallWarningReachesOnlyOwnLogger(CancellationToken cancellationToken)
    {
        using var recorderA = new RecordingLoggerProvider();
        using var recorderB = new RecordingLoggerProvider();
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        await using var clusterA = await StartClusterAsync("node-a", ClockOptions("log-isolation-stall-a", recorderA, clock), cancellationToken);
        await using var clusterB = await StartClusterAsync("node-b", Options("log-isolation-stall-b", true, recorderB), cancellationToken);
        if (clusterA["node-a"].GetRequiredService<JournalCoordinatorHost>().Coordinator is not IJournalStallProbeSource journal)
            throw new InvalidOperationException("the host journal does not expose its stall probe.");

        // The node is idle, so its journal thread performs no segment I/O and the test is the only writer of the probe.
        journal.StallProbe.IoStarted(nameof(IJournalSegmentWriter.FlushToDisk));
        try
        {
            clock.Advance(TimeSpan.FromMilliseconds(JournalSlowOperationReporter.WarningThresholdMs + 1));

            journal.StallProbe.ReportWaitCanceled("durability commit");
        }
        finally
        {
            journal.StallProbe.IoFinished();
        }

        _ = await Assert.That(recorderA.Find(JournalWaitCanceledWhileStalledEventId)?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(recorderB.Find(JournalWaitCanceledWhileStalledEventId)).IsNull();
    }

    private static async Task AssertRepairLoggedOnlyForAsync(RecordingLoggerProvider recorder, string ownPath, string otherPath)
    {
        var logged = recorder.Find(TornTailTruncatedEventId);
        _ = await Assert.That(logged?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(logged?.Category).IsEqualTo(typeof(JournalCoordinatorHost).FullName);
        _ = await Assert.That(logged?.Message).Contains(ownPath, StringComparison.Ordinal);
        foreach (var entry in recorder.Snapshot())
            _ = await Assert.That(entry.Message).DoesNotContain(otherPath, StringComparison.Ordinal);
    }

    private static IntegrationStartOptions ClockOptions(string scope, RecordingLoggerProvider recorder, TimeProvider clock) => new()
    {
        UsePersistence = true,
        ExtraScope = scope,
        ServicesConfigure = recorder.Register,
        TimeProvider = clock,
    };

    private static IntegrationStartOptions Options(string scope, bool clean, RecordingLoggerProvider? recorder) => new()
    {
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        ServicesConfigure = recorder == null ? null : recorder.Register,
    };

    private async Task<string> PrepareTornSegmentAsync(string nodeId, string scope, CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(nodeId, Options(scope, true, null), cancellationToken);
        var node = cluster[nodeId];
        var cache = node.GetCache<object?>(CacheName);
        for (var i = 1; i <= 2; i++)
        {
            var entry = new NodeCacheEntry<object?> { Value = $"value-{i}", Version = i };
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, $"isolation-key-{i}", entry, cancellationToken);
        }

        await cluster.StopNodeAsync(nodeId);
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
        var persistence = new PersistenceOptions { DataDir = node.DataDir, JournalMaxSegmentMb = 16 };
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        var manifest = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        var path = NodePathKit.Combine(node.DataDir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(manifest.CurrentJournal)}{FileExtensions.Journal}");
        var intact = await File.ReadAllBytesAsync(path, cancellationToken);
        byte[] torn = [.. intact, .. intact.AsSpan(JournalFraming.FileHeaderSize, 3)];
        await File.WriteAllBytesAsync(path, torn, cancellationToken);
        return path;
    }
}
