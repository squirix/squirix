using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The journal byte quota of a node at its limit: retention frees the segments a snapshot covers.</summary>
[Immutable]
public sealed class JournalRetentionQuotaTests : IsolatedStorageTestBase
{
    private const int FillBoundEntries = 60;
    private const int FillValueLength = 100 * 1024;
    private const int QuotaMb = 2;
    private const int SnapshotIndex = 1;

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-retention-quota";

    /// <summary>
    /// A node at its journal quota is admitted again once a snapshot lets retention delete the covered segments, with no append in between to
    /// trigger the counter read, and the counters never fall below what is on disk.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetentionFreesQuotaWithoutAppend(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter, journalMaxTotalBytesMb: QuotaMb);
        await using var session = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken);
        var coordinator = (await Assert.That(session.Journal).IsTypeOf<JournalCoordinator>())!;
        _ = await FillUntilRefusedAsync(session, cancellationToken);
        var usedAtQuota = session.Journal.UsedBytes;
        var replayFrom = coordinator.CurrentSegmentIndex;
        _ = await Assert.That(replayFrom).IsGreaterThan(1);

        await PublishSnapshotAsync(session, kit, replayFrom, coordinator.NextSequence, cancellationToken);
        await coordinator.WaitUntilAsync(j => j.UsedBytes < usedAtQuota - (FillValueLength * 2L), Bound, TimeProvider.System, cancellationToken);

        _ = await Assert.That(session.Journal.UsedBytes).IsGreaterThanOrEqualTo(OnDiskBytes());
        _ = await Assert.That(JournalReadPath.EnumerateSegments(Dir, 1).Length).IsEqualTo(1);
        await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "after-retention", new NodeCacheEntry<string>("v"), cancellationToken);
    }

    /// <summary>After retention deleted the covered segments a restart recovers the snapshot and the tail that follows it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartAfterRetentionRecoversTail(CancellationToken cancellationToken)
    {
        var kit = new JournalReplayKit(Dir, _testMeter, journalMaxTotalBytesMb: QuotaMb);
        await using (var session = await kit.OpenAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken))
        {
            var coordinator = (await Assert.That(session.Journal).IsTypeOf<JournalCoordinator>())!;
            _ = await FillUntilRefusedAsync(session, cancellationToken);
            var usedAtQuota = session.Journal.UsedBytes;
            await PublishSnapshotAsync(session, kit, coordinator.CurrentSegmentIndex, coordinator.NextSequence, cancellationToken);
            await coordinator.WaitUntilAsync(j => j.UsedBytes < usedAtQuota - (FillValueLength * 2L), Bound, TimeProvider.System, cancellationToken);
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, "tail", new NodeCacheEntry<string>("tail-value"), cancellationToken);
            await session.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var recovered = await kit.RecoverAsync(JournalReplayKit.CreateRestartClock(), cancellationToken);

        _ = await Assert.That((await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "snap-0"), cancellationToken))?.Value).IsNotNull();
        _ = await Assert.That((await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "tail"), cancellationToken))?.Value).IsEqualTo("tail-value");
        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(JournalReplayKit.CacheName, "fill-0"), cancellationToken)).IsNull();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static async Task<int> FillUntilRefusedAsync(JournalReplayKit.Session session, CancellationToken cancellationToken)
    {
        var value = new string('x', FillValueLength);
        for (var i = 0; i < FillBoundEntries; i++)
        {
            try
            {
                await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, JournalReplayKit.CacheName, $"fill-{i}", new NodeCacheEntry<string>(value), cancellationToken);
            }
            catch (JournalCapacityExceededException)
            {
                return i;
            }
        }

        throw new InvalidOperationException("the journal quota never refused a write.");
    }

    private static Task PublishSnapshotAsync(JournalReplayKit.Session session, JournalReplayKit kit, int replayFromSegment, ulong nextSequence, CancellationToken cancellationToken)
    {
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>
        {
            (new CacheKey(JournalReplayKit.CacheName, "snap-0"), new NodeCacheEntry<object?> { Value = "snapshot-value" }),
        };
        return PublishAsync(session.ManifestStore, kit, items, replayFromSegment, nextSequence, cancellationToken);
    }

    private static async Task PublishAsync(
        Ledger ledger,
        JournalReplayKit kit,
        List<(CacheKey Key, NodeCacheEntry<object?> Entry)> items,
        int replayFromSegment,
        ulong nextSequence,
        CancellationToken cancellationToken)
    {
        _ = await StoreFactory.CreateWriter(kit.Persistence).WriteAsync(SnapshotIndex, items, [], cancellationToken);
        await ledger.WriteSnapshotAsync(
            new State
            {
                CurrentJournal = replayFromSegment,
                NextSequence = nextSequence,
                LastSnapshot = new SnapshotRef
                {
                    Index = SnapshotIndex,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = nextSequence - 1UL,
                    ReplayFromJournalSegment = replayFromSegment,
                },
            },
            cancellationToken);
    }

    private long OnDiskBytes()
    {
        long bytes = 0;
        foreach (var segment in JournalReadPath.EnumerateSegments(Dir, 1))
            bytes += new FileInfo(segment.Path).Length;

        return bytes;
    }
}
