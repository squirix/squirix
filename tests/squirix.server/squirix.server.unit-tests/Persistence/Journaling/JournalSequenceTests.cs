using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>The journal sequence counter always names the next sequence to allocate, so frames stay contiguous and the snapshot watermark is the last applied frame.</summary>
[Immutable]
public sealed class JournalSequenceTests : IsolatedStorageTestBase
{
    /// <summary>The first frame of a fresh journal gets sequence one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshJournalStartsAtOne(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence();
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        ulong nextBefore;
        await using (var journal = CreateJournal(persistence, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger))
        {
            nextBefore = journal.NextSequence;
            await AppendAsync(journal, "a", cancellationToken);
        }

        _ = await Assert.That(nextBefore).IsEqualTo(1UL);
        _ = await Assert.That(Join(ReadSequences(persistence.DataDir, 1, cancellationToken))).IsEqualTo("1");
    }

    /// <summary>A cut right after writes names the last frame as its watermark, and nothing is left above it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutWatermarkIsLastFrame(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence();
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        ulong watermark;
        ulong lastAllocated;
        await using (var journal = CreateJournal(persistence, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger))
        {
            await AppendAsync(journal, "a", cancellationToken);
            await AppendAsync(journal, "b", cancellationToken);
            await AppendAsync(journal, "c", cancellationToken);
            watermark = await CutAsync(journal, cancellationToken);
            lastAllocated = journal.NextSequence - 1UL;
        }

        var sequences = ReadSequences(persistence.DataDir, 1, cancellationToken);
        _ = await Assert.That(watermark).IsEqualTo(3UL);
        _ = await Assert.That(watermark).IsEqualTo(sequences[^1]);
        _ = await Assert.That(lastAllocated).IsEqualTo(watermark);
    }

    /// <summary>
    /// Restoring a snapshot at the cut watermark replays no frame: the snapshot deliberately lacks the key of the watermark frame, so any
    /// replay of that frame would bring the key back.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestoreAtWatermarkReplaysNothing(CancellationToken cancellationToken)
    {
        ulong watermark;
        await using (var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken))
        {
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("a"), cancellationToken);
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("b"), JournalEntryPayloadKit.EncodePut("b"), cancellationToken);
            await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
            watermark = await CutAsync(journal.Journal, cancellationToken);
            await journal.ShutdownAsync();

            var snapshot = StallableJournal.Describe([CacheKey.Default("a").ToString()]);
            _ = await Assert.That(journal.Recover(snapshot, watermark, cancellationToken)).IsEqualTo(snapshot);
            _ = await Assert.That(journal.Recover(snapshot, watermark - 1UL, cancellationToken)).IsEqualTo(StallableJournal.Describe([CacheKey.Default("a").ToString(), CacheKey.Default("b").ToString()]));
        }

        _ = await Assert.That(watermark).IsEqualTo(2UL);
    }

    /// <summary>A restart after a roll whose new segment is still empty continues above the highest frame on disk.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartAfterRollWithEmptySegment(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence();
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(
            Dir,
            1,
            [
                BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "a", "v"),
                BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "b", "v"),
                BinaryJournalTestSegmentWriter.BuildPutRecord(3UL, "c", "v"),
            ]);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, []);
        await ledger.WriteAsync(new State { Format = 1, CurrentJournal = 2, NextSequence = 4 }, cancellationToken);

        await using (var journal = CreateJournal(persistence, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger))
        {
            _ = await Assert.That(journal.NextSequence).IsEqualTo(4UL);
            await AppendAsync(journal, "d", cancellationToken);
        }

        var sequences = ReadSequences(persistence.DataDir, 1, cancellationToken);
        _ = await Assert.That(sequences[^1]).IsEqualTo(4UL);
    }

    /// <summary>The first frame after a restart gets the highest recovered sequence plus one, leaving no gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartLeavesNoGap(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence();
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        await using (var first = CreateJournal(persistence, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger))
        {
            await AppendAsync(first, "a", cancellationToken);
            await AppendAsync(first, "b", cancellationToken);
        }

        await using (var second = CreateJournal(persistence, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger))
            await AppendAsync(second, "c", cancellationToken);

        _ = await Assert.That(Join(ReadSequences(persistence.DataDir, 1, cancellationToken))).IsEqualTo("1,2,3");
    }

    /// <summary>The first frame after a maintenance end continues directly after the last frame.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceLeavesNoGap(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence();
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        await using (var journal = CreateJournal(persistence, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger))
        {
            await AppendAsync(journal, "a", cancellationToken);
            await AppendAsync(journal, "b", cancellationToken);
            await journal.ExecuteMaintenanceExclusiveAsync(static _ => ValueTask.CompletedTask, cancellationToken);
            await AppendAsync(journal, "c", cancellationToken);
        }

        _ = await Assert.That(Join(ReadSequences(persistence.DataDir, 1, cancellationToken))).IsEqualTo("1,2,3");
    }

    /// <summary>The compacted segment is numbered above the compacted frames, and writes after a restart continue it without a gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionRestartStaysContiguous(CancellationToken cancellationToken)
    {
        var persistence = NewPersistence();
        using var ledger = new Ledger(persistence, NullLogger<Ledger>.Instance);
        await using (var first = CreateJournal(persistence, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger))
        {
            await AppendAsync(first, "a", cancellationToken);
            await AppendAsync(first, "b", cancellationToken);
            await AppendAsync(first, "c", cancellationToken);
        }

        await JournalCompactor.CompactAsync(persistence, ledger, StoreFactory.CreateReader(), DateTime.UtcNow, DateTime.UtcNow, cancellationToken);
        var compacted = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        await using (var second = CreateJournal(persistence, compacted, ledger))
        {
            await AppendAsync(second, "d", cancellationToken);
            await AppendAsync(second, "e", cancellationToken);
        }

        _ = await Assert.That(compacted.NextSequence).IsEqualTo(7UL);
        _ = await Assert.That(Join(ReadSequences(persistence.DataDir, compacted.CurrentJournal, cancellationToken))).IsEqualTo("4,5,6,7,8");
    }

    private static async Task AppendAsync(IJournalCoordinator journal, string key, CancellationToken cancellationToken)
    {
        await journal.AppendPutUnderGateAsync(CacheKey.Default(key), JournalEntryPayloadKit.EncodePut(key), cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
    }

    private static ValueTask<ulong> CutAsync(IJournalCoordinator journal, CancellationToken cancellationToken) => journal.ExecuteSnapshotCutAsync(
        0,
        static (_, sequence, _) => ValueTask.FromResult(sequence),
        static (_, sequence, _, _) => ValueTask.FromResult(sequence),
        cancellationToken);

    private static string Join(List<ulong> sequences)
    {
        var builder = new StringBuilder();
        foreach (var sequence in CollectionsMarshal.AsSpan(sequences))
        {
            if (builder.Length > 0)
                _ = builder.Append(',');

            _ = builder.Append(sequence);
        }

        return builder.ToString();
    }

    private static List<ulong> ReadSequences(string dataDir, int fromSegment, CancellationToken cancellationToken)
    {
        var sequences = new List<ulong>();
        using var records = JournalReadPath.ReadAll(dataDir, fromSegment, cancellationToken);
        while (records.MoveNext())
            sequences.Add(records.Current.Sequence);

        return sequences;
    }

    private static IJournalCoordinator CreateJournal(PersistenceOptions persistence, State manifest, Ledger ledger) => JournalCoordinatorFactory.Create(
        persistence,
        manifest,
        ledger,
        new AsyncManualResetEvent(true),
        NullLoggerFactory.Instance,
        TimeProvider.System,
        out _);

    private PersistenceOptions NewPersistence() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 16,
        ManifestRetentionCount = 1,
    };
}
