using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Win32.SafeHandles;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The compacted journal segment must be durable before the manifest switches to it.</summary>
[Immutable]
public sealed class JournalCompactorDurabilityTests : IsolatedStorageTestBase
{
    private const string CacheName = JournalReplayKit.CacheName;

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-compact-durability";

    /// <summary>The segment is flushed once, complete and still unpublished, and the compacted journal recovers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionFlushesSegmentBeforePublish(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        await WriteKeysAsync(kit, 3, cancellationToken);
        var finalPath = JournalReadPath.BuildSegmentPath(Dir, NextSegmentIndex());
        var flushes = 0;
        var publishedAtFlush = true;
        var lengthAtFlush = -1L;
        var sources = JournalReadPath.EnumerateSegments(Dir, 1);
        var manifestAtFlush = string.Empty;
        var sourcesAtFlush = false;
        using var ledger = new Ledger(kit.Persistence, NullLogger<Ledger>.Instance);
        var manifestBefore = ReadNonJournalFiles();
        var expectations = new ICompactedSegmentDurabilityCreateExpectations();
        _ = expectations.Setups.Flush(Arg.Any<SafeFileHandle>()).Callback(handle =>
        {
            flushes++;
            publishedAtFlush = File.Exists(finalPath);
            lengthAtFlush = RandomAccess.GetLength(handle);
            manifestAtFlush = ReadNonJournalFiles();
            sourcesAtFlush = Array.TrueForAll(sources, static source => File.Exists(source.Path));
        });

        await CompactAsync(kit, ledger, expectations.Instance(), cancellationToken);

        _ = await Assert.That(flushes).IsEqualTo(1);
        _ = await Assert.That(publishedAtFlush).IsFalse();
        _ = await Assert.That(new FileInfo(finalPath).Length).IsEqualTo(lengthAtFlush);
        _ = await Assert.That(manifestAtFlush).IsEqualTo(manifestBefore);
        _ = await Assert.That(sourcesAtFlush).IsTrue();
        var manifest = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(JournalReadPath.BuildSegmentPath(Dir, manifest.CurrentJournal)).IsEqualTo(finalPath);
        await AssertKeysRecoveredAsync(kit, 3, cancellationToken);
    }

    /// <summary>A segment that fails to flush is never published and the source segments stay authoritative.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedSegmentFlushKeepsSourceSegments(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        await WriteKeysAsync(kit, 3, cancellationToken);
        var sources = JournalReadPath.EnumerateSegments(Dir, 1);
        var finalPath = JournalReadPath.BuildSegmentPath(Dir, NextSegmentIndex());
        var expectations = new ICompactedSegmentDurabilityCreateExpectations();
        _ = expectations.Setups.Flush(Arg.Any<SafeFileHandle>()).Callback(static _ => throw new IOException("flush failed"));

        using var ledger = new Ledger(kit.Persistence, NullLogger<Ledger>.Instance);
        var before = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(CompactAsync(kit, ledger, expectations.Instance(), cancellationToken));

        var after = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(File.Exists(finalPath)).IsFalse();
        _ = await Assert.That(after.CurrentJournal).IsEqualTo(before.CurrentJournal);
        _ = await Assert.That(after.NextSequence).IsEqualTo(before.NextSequence);
        foreach (var source in sources)
            _ = await Assert.That(File.Exists(source.Path)).IsTrue();

        await AssertKeysRecoveredAsync(kit, 3, cancellationToken);

        await JournalCompactor.CompactAsync(kit.Persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, null, cancellationToken);
        _ = await Assert.That(File.Exists(finalPath)).IsTrue();
        await AssertKeysRecoveredAsync(kit, 3, cancellationToken);
    }

    /// <summary>A compaction that writes only the header is flushed too.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyCompactionStillFlushes(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        await WriteKeysAsync(kit, 0, cancellationToken);
        var finalPath = JournalReadPath.BuildSegmentPath(Dir, NextSegmentIndex());
        var flushes = 0;
        var publishedAtFlush = true;
        var lengthAtFlush = -1L;
        var expectations = new ICompactedSegmentDurabilityCreateExpectations();
        _ = expectations.Setups.Flush(Arg.Any<SafeFileHandle>()).Callback(handle =>
        {
            flushes++;
            publishedAtFlush = File.Exists(finalPath);
            lengthAtFlush = RandomAccess.GetLength(handle);
        });

        using var ledger = new Ledger(kit.Persistence, NullLogger<Ledger>.Instance);
        await CompactAsync(kit, ledger, expectations.Instance(), cancellationToken);

        _ = await Assert.That(flushes).IsEqualTo(1);
        _ = await Assert.That(publishedAtFlush).IsFalse();
        _ = await Assert.That(lengthAtFlush).IsEqualTo(JournalFraming.FileHeaderSize);
        _ = await Assert.That(File.Exists(finalPath)).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static Task CompactAsync(JournalReplayKit kit, Ledger ledger, ICompactedSegmentDurability durability, CancellationToken cancellationToken) =>
        JournalCompactor.CompactAsync(kit.Persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, null, durability, cancellationToken);

    private static string KeyName(int index) => $"key-{index}";

    private static async Task WriteKeysAsync(JournalReplayKit kit, int count, CancellationToken cancellationToken)
    {
        await using var session = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken);
        for (var i = 0; i < count; i++)
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, KeyName(i), new NodeCacheEntry<string>("v"), cancellationToken);
    }

    private static async Task AssertKeysRecoveredAsync(JournalReplayKit kit, int count, CancellationToken cancellationToken)
    {
        var recovered = await kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), false, cancellationToken);
        for (var i = 0; i < count; i++)
        {
            var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, KeyName(i)), cancellationToken);
            _ = await Assert.That(entry?.Value).IsEqualTo("v");
        }
    }

    private string ReadNonJournalFiles()
    {
        var builder = new StringBuilder();
        foreach (var path in Directory.GetFiles(Dir, "*", SearchOption.AllDirectories))
        {
            if (Path.GetFileName(path).StartsWith(FilePrefixes.Journal, StringComparison.Ordinal))
                continue;

            var info = new FileInfo(path);
            _ = builder.Append(path).Append(':').Append(info.Length).Append(':').Append(info.LastWriteTimeUtc.Ticks).Append(';');
        }

        return builder.ToString();
    }

    private int NextSegmentIndex()
    {
        var segments = JournalReadPath.EnumerateSegments(Dir, 1);
        return segments.Length == 0 ? 1 : segments[^1].Index + 1;
    }
}
