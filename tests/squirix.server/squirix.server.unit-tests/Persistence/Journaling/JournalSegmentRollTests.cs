using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal segment roll happens before the frame that would overflow the active segment.</summary>
[Immutable]
public sealed class JournalSegmentRollTests : IsolatedStorageTestBase
{
    private const int FillPayloadSize = 8_192;
    private const int LargePayloadSize = 16_000;

    /// <summary>When the next manifest file cannot be created, the roll fails before the overflow frame is appended.</summary>
    [Fact]
    public async Task BlockedManifestStillAppendsFrames()
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        var segmentOnePath = SegmentPath(Dir, 1);
        var bytesBefore = new FileInfo(segmentOnePath).Length;

        Exception? rollError = null;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ledger.EnqueueRoll(
            1,
            1,
            () => done.TrySetResult(),
            ex =>
            {
                rollError = ex;
                _ = done.TrySetResult();
            });
        await done.Task;
        rollError.ThrowIfFaulted();

        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(2)), [], DefaultCancellationToken);
        var block = CountManifestDataFiles(Dir);
        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), DefaultCancellationToken);
        Assert.True(journal.HasFlushLoopFailure);
        Assert.Equal(bytesBefore, new FileInfo(segmentOnePath).Length);
        Assert.Equal(block, CountManifestDataFiles(Dir));
        Assert.False(ContainsPutKey(Dir, 1, "overflow-key"));
        Assert.False(ContainsPutKey(Dir, 2, "overflow-key"));
    }

    /// <summary>An overflow frame is written only after a successful roll, on the new journal segment file.</summary>
    [Fact]
    public async Task OverflowingAppendLandsOnNextRoll()
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        await ledger.WaitUntilValueAsync(ConditionAsync, DefaultCancellationToken);

        Assert.Equal(2, (await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        Assert.False(ContainsPutKey(Dir, 1, "overflow-key"));
        Assert.True(ContainsPutKey(Dir, 2, "overflow-key"));
        return;

        static async ValueTask<bool> ConditionAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>The roll target segment is created durably before the roll manifest is published.</summary>
    [Fact]
    public async Task RollPrecreatesSegmentBeforeManifest()
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        Exception? rollError = null;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        ledger.EnqueueRoll(
            1,
            1,
            () => done.TrySetResult(),
            ex =>
            {
                rollError = ex;
                _ = done.TrySetResult();
            });
        await done.Task;
        rollError.ThrowIfFaulted();

        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(2)), [], DefaultCancellationToken);
        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), DefaultCancellationToken);
        Assert.True(journal.HasFlushLoopFailure);

        // The manifest still advertises segment 1, but the roll already materialized segment 2.
        Assert.Equal(1, (await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        var segmentTwoPath = SegmentPath(Dir, 2);
        Assert.True(File.Exists(segmentTwoPath));
        var header = await File.ReadAllBytesAsync(segmentTwoPath, DefaultCancellationToken);
        Assert.True(header.Length >= JournalFraming.FileHeaderSize);
        Assert.True(header.AsSpan(0, 4).SequenceEqual("SJRN"u8));
        Assert.Equal(JournalFraming.Version, header[4]);
    }

    /// <summary>After a restart, a roll reuses a pre-created header-only target without double-counting it.</summary>
    [Fact]
    public async Task RestartReusesPrecreatedRollTarget()
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using (var journal = JournalCoordinatorFactory.Create(
                         options,
                         await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
                         ledger,
                         new AsyncManualResetEvent(true)))
        {
            var pipelined = Assert.IsType<JournalCoordinator>(journal);
            var overflowPayload = new byte[LargePayloadSize];
            Array.Fill(overflowPayload, Convert.ToByte('y'));
            var overflowFrameLen = FrameLength(overflowPayload, CacheKey.Default("overflow-key"));
            await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);
        }

        // Simulate the crash aftermath: a pre-created header-only segment 2 with the manifest still on 1.
        var segmentTwoPath = SegmentPath(Dir, 2);
        WriteHeaderOnlySegment(segmentTwoPath);

        await using var restarted = JournalCoordinatorFactory.Create(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new AsyncManualResetEvent(true));
        var restartedPipelined = Assert.IsType<JournalCoordinator>(restarted);
        Assert.Equal(1, restartedPipelined.CurrentSegmentIndex);

        var payload = new byte[LargePayloadSize];
        Array.Fill(payload, Convert.ToByte('y'));
        var key = CacheKey.Default("overflow-key");
        await restarted.AppendPutAsync(key, payload, DefaultCancellationToken);
        await restarted.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        await ledger.WaitUntilValueAsync(RolledToTwoAsync, DefaultCancellationToken);

        Assert.Equal(2, (await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        Assert.Equal(2, restartedPipelined.CurrentSegmentIndex);
        Assert.True(ContainsPutKey(Dir, 2, "overflow-key"));

        // No double counting: in-memory totals match the on-disk layout.
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        Assert.Equal(2, segmentCount);
        Assert.Equal(2, restartedPipelined.EventLoop.JournalSegmentCount);
        Assert.Equal(totalBytes, restartedPipelined.UsedBytes);
        Assert.Empty(Directory.GetFiles(Dir, "*.tmp"));
        return;

        static async ValueTask<bool> RolledToTwoAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>A torn pre-created roll target does not fail startup; it is repaired like the active segment.</summary>
    [Fact]
    public async Task TornPrecreatedTargetRepairedOnStartup()
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        var record = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k", "v");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, record);
        await ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2, LastSnapshot = null }, DefaultCancellationToken);

        // Torn roll target: 3 bytes, shorter than a valid header.
        await File.WriteAllBytesAsync(SegmentPath(Dir, 2), [0x53, 0x4A, 0x52], DefaultCancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);
        Assert.Equal(2UL, journal.NextSequence);
        Assert.Equal(1, pipelined.CurrentSegmentIndex);

        var repaired = await File.ReadAllBytesAsync(SegmentPath(Dir, 2), DefaultCancellationToken);
        Assert.Equal(JournalFraming.FileHeaderSize, repaired.Length);
        Assert.True(repaired.AsSpan(0, 4).SequenceEqual("SJRN"u8));
        Assert.Equal(JournalFraming.Version, repaired[4]);
    }

    /// <summary>Orphaned roll-target temp files are deleted on startup.</summary>
    [Fact]
    public async Task OrphanRollTempDeletedOnStartup()
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        var tmpPath = JournalReadPath.BuildRollTempPath(Dir, 5);
        await File.WriteAllBytesAsync(tmpPath, [0x01, 0x02], DefaultCancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        Assert.False(File.Exists(tmpPath));
    }

    /// <summary>A roll replaces a torn pre-created target; only the replacement delta enters the totals.</summary>
    [Fact]
    public async Task RollReplacesTornPrecreatedTarget()
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        // Foreign torn leftover at the roll target (2 bytes, created after startup stats).
        const int tornLength = 2;
        await File.WriteAllBytesAsync(SegmentPath(Dir, 2), [0x53, 0x4A], DefaultCancellationToken);

        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        await manifestStore.WaitUntilValueAsync(RolledToTwoAsync, DefaultCancellationToken);

        Assert.Equal(2, (await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        Assert.True(ContainsPutKey(Dir, 2, "overflow-key"));
        var segmentTwoCopyPath = NodePathKit.Combine(NodePathKit.Combine(Dir, "torn-replaced-reader"), "seg2-copy.jsqx");
        _ = Directory.CreateDirectory(NodePathKit.Combine(Dir, "torn-replaced-reader"));
        File.Copy(SegmentPath(Dir, 2), segmentTwoCopyPath, true);
        var segmentTwo = await File.ReadAllBytesAsync(segmentTwoCopyPath, DefaultCancellationToken);
        Assert.True(segmentTwo.AsSpan(0, 4).SequenceEqual("SJRN"u8));
        Assert.Equal(JournalFraming.Version, segmentTwo[4]);
        Assert.Empty(Directory.GetFiles(Dir, "*.tmp"));

        // The foreign torn bytes were never counted (single-writer model counts only writer-caused
        // growth), so the total lags on-disk by exactly those bytes.
        var (_, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        Assert.Equal(totalBytes - tornLength, pipelined.UsedBytes);
        return;

        static async ValueTask<bool> RolledToTwoAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>After a restart, a roll reuses a target that already holds frames without truncating it.</summary>
    [Fact]
    public async Task RestartReusesSegmentWithFrames()
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        await using (var journal = JournalCoordinatorFactory.Create(
                         options,
                         await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
                         manifestStore,
                         new AsyncManualResetEvent(true)))
        {
            var pipelined = Assert.IsType<JournalCoordinator>(journal);
            var overflowPayload = new byte[LargePayloadSize];
            Array.Fill(overflowPayload, Convert.ToByte('y'));
            var overflowFrameLen = FrameLength(overflowPayload, CacheKey.Default("overflow-key"));
            await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);
        }

        // Pre-existing segment above the manifest journal, holding a record (crash/foreign aftermath).
        var preexisting = BinaryJournalTestSegmentWriter.BuildPutRecord(100_000UL, "preexisting", "v");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, preexisting);

        await using var restarted = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var restartedPipelined = Assert.IsType<JournalCoordinator>(restarted);
        Assert.Equal(1, restartedPipelined.CurrentSegmentIndex);
        Assert.Equal(100_001UL, restarted.NextSequence);

        var payload = new byte[LargePayloadSize];
        Array.Fill(payload, Convert.ToByte('y'));
        await restarted.AppendPutAsync(CacheKey.Default("overflow-key"), payload, DefaultCancellationToken);
        await restarted.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        await manifestStore.WaitUntilValueAsync(RolledToTwoAsync, DefaultCancellationToken);

        Assert.Equal(2, (await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        Assert.True(ContainsPutKey(Dir, 2, "overflow-key"));
        Assert.True(ContainsPutKey(Dir, 2, "preexisting"));

        // Reuse adds nothing: in-memory totals match the on-disk layout exactly.
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        Assert.Equal(2, segmentCount);
        Assert.Equal(2, restartedPipelined.EventLoop.JournalSegmentCount);
        Assert.Equal(totalBytes, restartedPipelined.UsedBytes);
        return;

        static async ValueTask<bool> RolledToTwoAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>Startup creates a missing data directory instead of failing.</summary>
    [Fact]
    public async Task StartupCreatesMissingDataDir()
    {
        var dataDir = NodePathKit.Combine(Dir, "auto-created");
        var options = CreateOptions(dataDir);
        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            new State { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = null },
            manifestStore,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        await journal.AppendPutAsync(CacheKey.Default("k"), new byte[] { 1, 2, 3 }, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        Assert.True(Directory.Exists(dataDir));
        Assert.Equal(1, pipelined.CurrentSegmentIndex);
    }

    /// <summary>An existing zero-length segment does not fail startup.</summary>
    [Fact]
    public async Task ZeroLengthSegmentStartsUp()
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        await File.WriteAllBytesAsync(SegmentPath(Dir, 1), [], DefaultCancellationToken);
        await manifestStore.WriteAsync(
            new State { Format = 1, CurrentJournal = 1, NextSequence = 7, LastSnapshot = null },
            DefaultCancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        Assert.Equal(7UL, journal.NextSequence);
        Assert.Equal(1, pipelined.CurrentSegmentIndex);
    }

    private static bool ContainsPutKey(string dataDir, int segmentIndex, string key)
    {
        var path = SegmentPath(dataDir, segmentIndex);
        if (!File.Exists(path))
            return false;

        var isolatedDataDir = NodePathKit.Combine(dataDir, $"segment-reader-{NodeInvariantIndexStrings.Format(segmentIndex)}");
        _ = Directory.CreateDirectory(isolatedDataDir);
        File.Copy(path, JournalReadPath.BuildSegmentPath(isolatedDataDir, segmentIndex), true);

        using var enumerator = JournalReadPath.ReadAll(isolatedDataDir, segmentIndex, CancellationToken.None);
        while (enumerator.MoveNext())
        {
            var record = enumerator.Current;
            if (record.Operation is JournalOperationKind.Put && string.Equals(record.Key.Key, key, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    private static int CountManifestDataFiles(string dir) => Directory.Exists(dir) ? Directory.GetFiles(dir, $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}").Length : 0;

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 1,
        FlushInterval = 600_000,
        ManifestRetentionCount = 3,
    };

    private static async Task FillSegmentOneForOverflowAsync(JournalCoordinator journal, int overflowFrameLen, CancellationToken cancellationToken)
    {
        var fillPayload = new byte[FillPayloadSize];
        Array.Fill(fillPayload, Convert.ToByte('x'));
        var fillKey = CacheKey.Default("fill");
        var fillFrameLen = FrameLength(fillPayload, fillKey);
        const long maxBytes = 1024L * 1024L;

        for (var i = 0; i < 16_384 && journal.CurrentSegmentIndex == 1; i++)
        {
            if (journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes)
                break;

            if (journal.ActiveSegmentWrittenBytes + fillFrameLen > maxBytes)
                break;

            await journal.AppendPutAsync(fillKey, fillPayload, cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        Assert.Equal(1, journal.CurrentSegmentIndex);
        Assert.True(journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes);
    }

    private static int FrameLength(ReadOnlyMemory<byte> payload, CacheKey key)
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = key,
            PutEntryBytes = payload,
        };
        return JournalFraming.FrameTotalLength(BinaryJournalCodec.ComputeFrameBodyLength(record));
    }

    private static string SegmentPath(string dir, int i) => NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(i)}{FileExtensions.Journal}");

    private static void WriteHeaderOnlySegment(string path)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        RandomAccess.Write(handle, header, 0);
    }
}
