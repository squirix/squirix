using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
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

/// <summary>
/// Startup journal repairs are found while the journal opens. A real server start over a damaged segment must report them to the logger the host
/// registers, and must fail loudly, leaving the file untouched, when the damage is not provably safe to repair.
/// </summary>
public sealed class JournalStartupRepairLoggingTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const int HeaderRestoredEventId = 1016;
    private const int TornTailTruncatedEventId = 1018;

    /// <summary>A damaged magic in front of intact frames is restored on startup, the restored header is logged, and no entry is lost.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartLogsRestoredHeader(CancellationToken cancellationToken)
    {
        const string scope = "journal-repair-header";
        await using var cluster = await StartClusterAsync("node-a", Options(scope, true, null), cancellationToken);
        var node = cluster["node-a"];
        await WriteEntriesAsync(node, cancellationToken);
        await cluster.StopNodeAsync("node-a");
        var path = await ActiveSegmentPathAsync(node.DataDir, cancellationToken);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        bytes[0] = 0x42;
        bytes[1] = 0x41;
        bytes[2] = 0x44;
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);

        using var recorder = new RecordingLoggerProvider();
        var restarted = await cluster.StartNodeAsync("node-a", Options(scope, false, recorder), cancellationToken);

        var logged = recorder.Find(HeaderRestoredEventId);
        _ = await Assert.That(logged?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(logged?.Message).Contains(path, StringComparison.Ordinal);
        await AssertEntriesReadableAsync(restarted, cancellationToken);
        await restarted.ShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(restarted.DataDir, cancellationToken);
        var repaired = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(HasValidHeader(repaired)).IsTrue();
    }

    /// <summary>A torn last frame is truncated on startup, the truncation is logged, and the earlier entries stay readable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartLogsTruncatedTornTail(CancellationToken cancellationToken)
    {
        const string scope = "journal-repair-tail";
        await using var cluster = await StartClusterAsync("node-a", Options(scope, true, null), cancellationToken);
        var node = cluster["node-a"];
        await WriteEntriesAsync(node, cancellationToken);
        await cluster.StopNodeAsync("node-a");
        var path = await ActiveSegmentPathAsync(node.DataDir, cancellationToken);
        var intact = await File.ReadAllBytesAsync(path, cancellationToken);
        byte[] torn = [.. intact, .. intact.AsSpan(JournalFraming.FileHeaderSize, 3)];
        await File.WriteAllBytesAsync(path, torn, cancellationToken);

        using var recorder = new RecordingLoggerProvider();
        var restarted = await cluster.StartNodeAsync("node-a", Options(scope, false, recorder), cancellationToken);

        var logged = recorder.Find(TornTailTruncatedEventId);
        _ = await Assert.That(logged?.Level).IsEqualTo(LogLevel.Warning);
        _ = await Assert.That(logged?.Message).Contains(path, StringComparison.Ordinal);
        await AssertEntriesReadableAsync(restarted, cancellationToken);
        await restarted.ShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(restarted.DataDir, cancellationToken);
        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(intact.Length);
    }

    /// <summary>A damaged header together with a damaged first frame followed by data fails the node start and leaves the segment untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartFailsOnUnrepairableSegment(CancellationToken cancellationToken)
    {
        const string scope = "journal-repair-fail";
        await using var cluster = await StartClusterAsync("node-a", Options(scope, true, null), cancellationToken);
        var node = cluster["node-a"];
        await WriteEntriesAsync(node, cancellationToken);
        await cluster.StopNodeAsync("node-a");
        var path = await ActiveSegmentPathAsync(node.DataDir, cancellationToken);
        var damaged = await File.ReadAllBytesAsync(path, cancellationToken);
        damaged[0] = 0x42;
        damaged[JournalFraming.FileHeaderSize + JournalFraming.FrameHeaderSize] ^= 0xFF;
        await File.WriteAllBytesAsync(path, damaged, cancellationToken);

        var failure = await NodeAsyncAssert.ThrowsAnyAsync<InvalidDataException, ITestNodeHost>(cluster.StartNodeAsync("node-a", Options(scope, false, null), cancellationToken));

        _ = await Assert.That(failure.Message).Contains(path, StringComparison.Ordinal);
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(after.AsSpan().SequenceEqual(damaged)).IsTrue();
    }

    private static async Task<string> ActiveSegmentPathAsync(string dataDir, CancellationToken cancellationToken)
    {
        await JournalSegmentLeaseWait.WaitForReleasedAsync(dataDir, cancellationToken);
        var persistence = new PersistenceOptions { DataDir = dataDir, JournalMaxSegmentMb = 16 };
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        var manifest = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        return NodePathKit.Combine(dataDir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(manifest.CurrentJournal)}{FileExtensions.Journal}");
    }

    private static async Task AssertEntriesReadableAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        var cache = node.GetCache<object?>(CacheName);
        var first = await cache.GetValueAsync(CacheName, "repair-key-1", cancellationToken);
        var second = await cache.GetValueAsync(CacheName, "repair-key-2", cancellationToken);
        _ = await Assert.That((first.Found, first.Value)).IsEqualTo((true, "value-1"));
        _ = await Assert.That((second.Found, second.Value)).IsEqualTo((true, "value-2"));
    }

    private static IntegrationStartOptions Options(string scope, bool clean, RecordingLoggerProvider? recorder) => new()
    {
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        ServicesConfigure = recorder == null ? null : recorder.Register,
    };

    private static bool HasValidHeader(byte[] segment)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        return segment.AsSpan(0, header.Length).SequenceEqual(header);
    }

    private static async Task WriteEntriesAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        var cache = node.GetCache<object?>(CacheName);
        for (var i = 1; i <= 2; i++)
        {
            var entry = new NodeCacheEntry<object?> { Value = $"value-{i}", Version = i };
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, $"repair-key-{i}", entry, cancellationToken);
        }
    }
}
