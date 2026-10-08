using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A fused write keeps its guarantees while it is parked in the flush, across segment rolls, compaction and a graceful stop.</summary>
[Immutable]
public sealed class FusedWriteDurabilityTests : IsolatedStorageTestBase
{
    private const int SegmentBytes = 1024 * 1024;

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly CacheKey SharedKey = new(FusedWriteHarness.CacheName, FusedWriteHarness.Key);

    /// <summary>While the flush is parked both frames are on the ring, nothing is visible, and a retry joins the write; the release applies and answers once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ParkedWriteAppliesAfterFlush(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);
        journal.Writer.Flush.Arm();

        var write = harness.SetThroughScopeAsync(operationId, "a", cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var framesWhileParked = harness.ReadFrames(cancellationToken);
        var visibleWhileParked = (await harness.Physical.GetValueAsync(SharedKey, cancellationToken)).Found;
        var retry = harness.SetThroughScopeAsync(operationId, "a", cancellationToken);
        var retryJoined = await PendingProbe.StaysPendingAsync(retry);
        journal.Writer.Flush.Release();
        var response = await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var replayed = await retry.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await Assert.That(framesWhileParked).Count().IsEqualTo(2);
        _ = await Assert.That(visibleWhileParked).IsFalse();
        _ = await Assert.That(retryJoined).IsTrue();
        _ = await Assert.That(replayed).IsEqualTo(response);
        _ = await Assert.That((await harness.Physical.GetValueAsync(SharedKey, cancellationToken)).Found).IsTrue();
    }

    /// <summary>A compaction that runs while the write is parked rewrites both frames, and the restart still holds the effect and the replayable outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionWhileParkedKeepsBoth(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);
        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 4, ManifestRetentionCount = 1 };
        journal.Writer.Flush.Arm();

        var write = harness.SetThroughScopeAsync(operationId, "a", cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var compaction = journal.Journal.ExecuteMaintenanceExclusiveAsync(
            ct => new ValueTask(JournalCompactor.CompactAsync(persistence, journal.Ledger, StoreFactory.CreateReader(), harness.Clock.GetUtcNow().UtcDateTime, harness.Clock.GetUtcNow().UtcDateTime, ct)),
            cancellationToken).AsTask();
        journal.Writer.Flush.Release();
        _ = await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await compaction.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await journal.ShutdownAsync();
        var (memory, store) = await harness.RecoverAsync(cancellationToken);

        _ = await Assert.That((await memory.GetValueAsync(SharedKey, cancellationToken)).Found).IsTrue();
        _ = await Assert.That(store.TryReplay(operationId, "fp", SetAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A graceful stop completes a parked write: the final flush covers both frames, so it applies and succeeds, and the restart replays it.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GracefulStopCompletesParkedWrite(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);
        journal.Writer.Flush.Arm();

        var write = harness.SetThroughScopeAsync(operationId, "a", cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        await journal.ShutdownAsync();
        _ = await write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var (memory, store) = await harness.RecoverAsync(cancellationToken);

        _ = await Assert.That((await memory.GetValueAsync(SharedKey, cancellationToken)).Found).IsTrue();
        _ = await Assert.That(store.TryReplay(operationId, "fp", SetAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A segment roll between the mutation frame and the outcome frame: a restart that sees both replays the effect and the outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RollBetweenFramesReplaysBoth(CancellationToken cancellationToken)
    {
        var operationId = FusedWriteHarness.OpId(1);
        var harness = await WriteAcrossRollAsync(Path.Join(Dir, "run"), operationId, cancellationToken);

        var (memory, store) = await harness.RecoverAsync(cancellationToken);

        _ = await Assert.That((await memory.GetValueAsync(SharedKey, cancellationToken)).Found).IsTrue();
        _ = await Assert.That(store.TryReplay(operationId, "fp", SetAsyncResponse.Parser, out _)).IsTrue();
    }

    /// <summary>A crash after the mutation frame is durable but before the outcome frame's segment is: the restart holds the effect and an unknown outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RollWithLostOutcomeKeepsUnknown(CancellationToken cancellationToken)
    {
        var operationId = FusedWriteHarness.OpId(1);
        var harness = await WriteAcrossRollAsync(Path.Join(Dir, "run"), operationId, cancellationToken);
        var segments = JournalReadPath.EnumerateSegments(harness.Dir, 1);
        using (var truncated = File.OpenHandle(segments[^1].Path, FileMode.Open, FileAccess.Write, FileShare.None))
            RandomAccess.SetLength(truncated, JournalFraming.FileHeaderSize);

        var (memory, store) = await harness.RecoverAsync(cancellationToken);

        _ = await Assert.That((await memory.GetValueAsync(SharedKey, cancellationToken)).Found).IsTrue();
        _ = await Assert.That(store.TryReplay(operationId, "fp", SetAsyncResponse.Parser, out _)).IsFalse();
        _ = await Assert.That(store.RecordCount).IsEqualTo(1);
    }

    private static async Task<List<int>> ReadFrameSizesAsync(string segmentPath, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(segmentPath, cancellationToken);
        var sizes = new List<int>();
        for (var offset = JournalFraming.FileHeaderSize; offset < bytes.Length;)
        {
            var total = JournalFraming.FrameTotalLength(BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(offset)));
            sizes.Add(total);
            offset += total;
        }

        return sizes;
    }

    /// <summary>Measures, in a scratch journal, the frame sizes of a filler write and of a fused write behind it.</summary>
    /// <param name="dir">Empty scratch directory.</param>
    /// <param name="fillerLength">Length of the filler value.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The sizes of the filler frame, the mutation frame and the outcome frame.</returns>
    private static async Task<(int Filler, int Mutation, int Outcome)> MeasureFrameSizesAsync(string dir, int fillerLength, CancellationToken cancellationToken)
    {
        _ = Directory.CreateDirectory(dir);
        await using var journal = await StallableJournal.CreateAsync(dir, false, 16, 1, 8, cancellationToken);
        var harness = new FusedWriteHarness(dir, journal);
        await WriteFillerAsync(harness, fillerLength, cancellationToken);
        _ = await harness.SetThroughScopeAsync(FusedWriteHarness.OpId(1), "a", cancellationToken);
        await journal.ShutdownAsync();

        var sizes = await ReadFrameSizesAsync(JournalReadPath.EnumerateSegments(dir, 1)[0].Path, cancellationToken);
        return (sizes[0], sizes[1], sizes[2]);
    }

    private static Task WriteFillerAsync(FusedWriteHarness harness, int length, CancellationToken cancellationToken) =>
        harness.Cache.SetEntryAsync(FusedWriteHarness.OpId(0), FusedWriteHarness.CacheName, "filler", new NodeCacheEntry<object?>(new string('x', length)), cancellationToken).AsTask();

    /// <summary>Writes a filler and then a fused write that leaves its outcome frame just beyond the end of the first segment, and stops the journal.</summary>
    /// <param name="dir">Empty directory of the run.</param>
    /// <param name="operationId">Operation id of the fused write.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The harness of the stopped journal.</returns>
    private async Task<FusedWriteHarness> WriteAcrossRollAsync(string dir, string operationId, CancellationToken cancellationToken)
    {
        // The same writes in a scratch journal give the exact frame sizes; the filler is then sized so the mutation frame ends within half an
        // outcome frame of the segment end, which makes the journal thread roll for the outcome frame alone.
        const int probeLength = 1_000_000;
        var (fillerFrame, mutationFrame, outcomeFrame) = await MeasureFrameSizesAsync(Path.Join(Dir, "measure"), probeLength, cancellationToken);
        var fillerLength = probeLength + (SegmentBytes - JournalFraming.FileHeaderSize - mutationFrame - (outcomeFrame / 2) - fillerFrame);

        _ = Directory.CreateDirectory(dir);
        var journal = await StallableJournal.CreateAsync(dir, false, 16, 1, 8, cancellationToken);
        var harness = new FusedWriteHarness(dir, journal);
        await WriteFillerAsync(harness, fillerLength, cancellationToken);
        _ = await harness.SetThroughScopeAsync(operationId, "a", cancellationToken);
        await journal.ShutdownAsync();

        var segments = JournalReadPath.EnumerateSegments(dir, 1);
        _ = await Assert.That(segments).Count().IsEqualTo(2);
        _ = await Assert.That(await ReadFrameSizesAsync(segments[0].Path, cancellationToken)).Count().IsEqualTo(2);
        _ = await Assert.That(await ReadFrameSizesAsync(segments[1].Path, cancellationToken)).Count().IsEqualTo(1);
        await journal.DisposeAsync();
        return harness;
    }
}
