using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Win32.SafeHandles;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
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
    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-compact-durability";

    /// <summary>The segment is flushed once, complete and still unpublished, and the compacted journal recovers.</summary>
    /// <param name="keys">The number of keys journaled before compaction.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(3)]
    [Arguments(0)]
    public async Task CompactionFlushesSegmentBeforePublish(int keys, CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        await WriteKeysAsync(kit, keys, cancellationToken);
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
        await JournalCompactor.CompactAsync(kit.Persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, null, expectations.Instance(), cancellationToken);

        _ = await Assert.That(flushes).IsEqualTo(1);
        _ = await Assert.That(publishedAtFlush).IsFalse();
        _ = await Assert.That(new FileInfo(finalPath).Length).IsEqualTo(lengthAtFlush);
        if (keys == 0)
            _ = await Assert.That(lengthAtFlush).IsEqualTo(JournalFraming.FileHeaderSize);

        await AssertKeysRecoveredAsync(kit, keys, cancellationToken);
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
        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(
            JournalCompactor.CompactAsync(kit.Persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, null, expectations.Instance(), cancellationToken));

        _ = await Assert.That(File.Exists(finalPath)).IsFalse();
        foreach (var source in sources)
            _ = await Assert.That(File.Exists(source.Path)).IsTrue();

        await JournalCompactor.CompactAsync(kit.Persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, null, cancellationToken);
        _ = await Assert.That(File.Exists(finalPath)).IsTrue();
        await AssertKeysRecoveredAsync(kit, 3, cancellationToken);
    }

    /// <summary>A compaction that fails at the manifest write leaves no segment above the manifest, so later writes stay authoritative.</summary>
    /// <param name="compactOnRecovery">Whether recovery compacts the journal before replaying it.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InterruptedCompactionKeepsLaterWrites(bool compactOnRecovery, CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        await using (var first = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken))
        {
            await first.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "K", new NodeCacheEntry<string>("v1"), cancellationToken);
            await first.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "J", new NodeCacheEntry<string>("j1"), cancellationToken);
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var expectations = new ICompactedSegmentDurabilityCreateExpectations();
        _ = expectations.Setups.Flush(Arg.Any<SafeFileHandle>()).Callback(_ => cts.CancelAfter(TimeSpan.Zero));
        using (var ledger = new Ledger(kit.Persistence, NullLogger<Ledger>.Instance))
        {
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(
                JournalCompactor.CompactAsync(kit.Persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, null, expectations.Instance(), cts.Token));
            _ = await Assert.That((await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(1);
        }

        _ = await Assert.That(File.Exists(JournalReadPath.BuildSegmentPath(Dir, 2))).IsFalse();

        await using (var second = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken))
        {
            await second.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "K", new NodeCacheEntry<string>("v2"), cancellationToken);
            await second.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "J", new NodeCacheEntry<string>("j2"), cancellationToken);
            _ = await second.Cache.RemoveAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "J", cancellationToken);
        }

        var recovered = await kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), compactOnRecovery, cancellationToken);
        _ = await Assert.That((await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "K"), cancellationToken))?.Value).IsEqualTo("v2");
        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "J"), cancellationToken)).IsNull();
    }

    /// <summary>A compaction that committed its manifest but did not publish its segment is rolled forward at startup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedCompactionRollsForward(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter);
        await WriteKeysAsync(kit, 3, cancellationToken);
        var segmentOne = JournalReadPath.BuildSegmentPath(Dir, 1);
        var segmentTwo = JournalReadPath.BuildSegmentPath(Dir, 2);
        var segmentOneCopy = Path.Join(Dir, "segment-one.copy");
        File.Copy(segmentOne, segmentOneCopy);
        using (var ledger = new Ledger(kit.Persistence, NullLogger<Ledger>.Instance))
            await JournalCompactor.CompactAsync(kit.Persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, null, cancellationToken);

        File.Move(segmentTwo, Path.Join(Dir, "jrn-000002.tmp"));
        File.Move(segmentOneCopy, segmentOne);

        await using (var session = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken))
        {
            _ = await Assert.That(File.Exists(segmentTwo)).IsTrue();
            _ = await Assert.That(File.Exists(Path.Join(Dir, "jrn-000002.tmp"))).IsFalse();
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "key-0", new NodeCacheEntry<string>("v2"), cancellationToken);
        }

        var recovered = await kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), false, cancellationToken);
        _ = await Assert.That((await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "key-0"), cancellationToken))?.Value).IsEqualTo("v2");
        _ = await Assert.That((await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "key-1"), cancellationToken))?.Value).IsEqualTo("v");
        _ = await Assert.That((await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "key-2"), cancellationToken))?.Value).IsEqualTo("v");
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static async Task AssertKeysRecoveredAsync(JournalReplayKit kit, int count, CancellationToken cancellationToken)
    {
        var recovered = await kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), false, cancellationToken);
        for (var i = 0; i < count; i++)
            _ = await Assert.That((await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, $"key-{i}"), cancellationToken))?.Value).IsEqualTo("v");
    }

    private static async Task WriteKeysAsync(JournalReplayKit kit, int count, CancellationToken cancellationToken)
    {
        await using var session = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken);
        for (var i = 0; i < count; i++)
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, $"key-{i}", new NodeCacheEntry<string>("v"), cancellationToken);
    }

    private int NextSegmentIndex()
    {
        var segments = JournalReadPath.EnumerateSegments(Dir, 1);
        return segments.Length == 0 ? 1 : segments[^1].Index + 1;
    }
}
