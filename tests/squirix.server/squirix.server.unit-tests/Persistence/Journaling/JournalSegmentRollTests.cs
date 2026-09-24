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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Exceptions;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal segment roll happens before the frame that would overflow the active segment.</summary>
[Immutable]
public sealed class JournalSegmentRollTests : IsolatedStorageTestBase
{
    private const int FillPayloadSize = 8_192;
    private const int LargePayloadSize = 16_000;

    /// <summary>When the next manifest file cannot be created, the roll fails before the overflow frame is appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedManifestStillAppendsFrames(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);

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

        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(2)), [], cancellationToken);
        var block = CountManifestDataFiles(Dir);
        await journal.AppendPutAsync(overflowKey, overflowPayload, cancellationToken);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), cancellationToken);
        _ = await Assert.That(journal.HasFlushLoopFailure).IsTrue();
        _ = await Assert.That(new FileInfo(segmentOnePath).Length).IsEqualTo(bytesBefore);
        _ = await Assert.That(CountManifestDataFiles(Dir)).IsEqualTo(block);
        _ = await Assert.That(ContainsPutKey(Dir, 1, "overflow-key")).IsFalse();
        _ = await Assert.That(ContainsPutKey(Dir, 2, "overflow-key")).IsFalse();
    }

    /// <summary>Orphaned roll-target temp files are deleted on startup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OrphanRollTempDeletedOnStartup(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        var tmpPath = JournalReadPath.BuildRollTempPath(Dir, 5);
        await File.WriteAllBytesAsync(tmpPath, ReadOnlyMemory<byte>.Of(0x01, 0x02), cancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        _ = await Assert.That(File.Exists(tmpPath)).IsFalse();
    }

    /// <summary>An overflow frame is written only after a successful roll, on the new journal segment file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OverflowingAppendLandsOnNextRoll(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);

        await journal.AppendPutAsync(overflowKey, overflowPayload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        await ledger.WaitUntilValueAsync(ConditionAsync, cancellationToken);

        _ = await Assert.That((await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(ContainsPutKey(Dir, 1, "overflow-key")).IsFalse();
        _ = await Assert.That(ContainsPutKey(Dir, 2, "overflow-key")).IsTrue();
        return;

        static async ValueTask<bool> ConditionAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>After a restart, a roll reuses a pre-created header-only target without double-counting it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartReusesPrecreatedRollTarget(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using (var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true)))
        {
            var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;
            var overflowPayload = new byte[LargePayloadSize];
            Array.Fill(overflowPayload, Convert.ToByte('y'));
            var overflowFrameLen = FrameLength(overflowPayload, CacheKey.Default("overflow-key"));
            await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);
        }

        // Simulate the crash aftermath: a pre-created header-only segment 2 with the manifest still on 1.
        var segmentTwoPath = SegmentPath(Dir, 2);
        WriteHeaderOnlySegment(segmentTwoPath);

        await using var restarted = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        var restartedPipelined = (await Assert.That(restarted).IsTypeOf<JournalCoordinator>())!;
        _ = await Assert.That(restartedPipelined.CurrentSegmentIndex).IsEqualTo(1);

        var payload = new byte[LargePayloadSize];
        Array.Fill(payload, Convert.ToByte('y'));
        var key = CacheKey.Default("overflow-key");
        await restarted.AppendPutAsync(key, payload, cancellationToken);
        await restarted.AwaitDurabilityCommitAsync(cancellationToken);

        await ledger.WaitUntilValueAsync(RolledToTwoAsync, cancellationToken);

        _ = await Assert.That((await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(restartedPipelined.CurrentSegmentIndex).IsEqualTo(2);
        _ = await Assert.That(ContainsPutKey(Dir, 2, "overflow-key")).IsTrue();

        // No double counting: in-memory totals match the on-disk layout.
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        _ = await Assert.That(segmentCount).IsEqualTo(2);
        _ = await Assert.That(restartedPipelined.EventLoop.JournalSegmentCount).IsEqualTo(2);
        _ = await Assert.That(restartedPipelined.UsedBytes).IsEqualTo(totalBytes);
        _ = await Assert.That(Directory.GetFiles(Dir, "*.tmp")).IsEmpty();
        return;

        static async ValueTask<bool> RolledToTwoAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>After a restart, a roll reuses a target that already holds frames without truncating it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartReusesSegmentWithFrames(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        await using (var journal = JournalCoordinatorFactory.Create(
                         options,
                         await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                         manifestStore,
                         new AsyncManualResetEvent(true)))
        {
            var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;
            var overflowPayload = new byte[LargePayloadSize];
            Array.Fill(overflowPayload, Convert.ToByte('y'));
            var overflowFrameLen = FrameLength(overflowPayload, CacheKey.Default("overflow-key"));
            await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);
        }

        // Pre-existing segment above the manifest journal, holding a record (crash/foreign aftermath).
        var preexisting = BinaryJournalTestSegmentWriter.BuildPutRecord(100_000UL, "preexisting", "v");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, preexisting);

        await using var restarted = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var restartedPipelined = (await Assert.That(restarted).IsTypeOf<JournalCoordinator>())!;
        _ = await Assert.That(restartedPipelined.CurrentSegmentIndex).IsEqualTo(1);
        _ = await Assert.That(restarted.NextSequence).IsEqualTo(100_001UL);

        var payload = new byte[LargePayloadSize];
        Array.Fill(payload, Convert.ToByte('y'));
        await restarted.AppendPutAsync(CacheKey.Default("overflow-key"), payload, cancellationToken);
        await restarted.AwaitDurabilityCommitAsync(cancellationToken);

        await manifestStore.WaitUntilValueAsync(RolledToTwoAsync, cancellationToken);

        _ = await Assert.That((await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(ContainsPutKey(Dir, 2, "overflow-key")).IsTrue();
        _ = await Assert.That(ContainsPutKey(Dir, 2, "preexisting")).IsTrue();

        // Reuse adds nothing: in-memory totals match the on-disk layout exactly.
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        _ = await Assert.That(segmentCount).IsEqualTo(2);
        _ = await Assert.That(restartedPipelined.EventLoop.JournalSegmentCount).IsEqualTo(2);
        _ = await Assert.That(restartedPipelined.UsedBytes).IsEqualTo(totalBytes);
        return;

        static async ValueTask<bool> RolledToTwoAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>The roll target segment is created durably before the roll manifest is published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RollPrecreatesSegmentBeforeManifest(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);

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

        await File.WriteAllBytesAsync(NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(2)), [], cancellationToken);
        await journal.AppendPutAsync(overflowKey, overflowPayload, cancellationToken);

        await pipelined.WaitUntilAsync(static j => j.HasFlushLoopFailure, TimeSpan.FromSeconds(15), cancellationToken);
        _ = await Assert.That(journal.HasFlushLoopFailure).IsTrue();

        // The manifest still advertises segment 1, but the roll already materialized segment 2.
        _ = await Assert.That((await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(1);
        var segmentTwoPath = SegmentPath(Dir, 2);
        _ = await Assert.That(File.Exists(segmentTwoPath)).IsTrue();
        var header = await File.ReadAllBytesAsync(segmentTwoPath, cancellationToken);
        _ = await Assert.That(header.Length >= JournalFraming.FileHeaderSize).IsTrue();
        _ = await Assert.That(header.AsSpan(0, 4).SequenceEqual("SJRN"u8)).IsTrue();
        _ = await Assert.That(header[4]).IsEqualTo(JournalFraming.Version);
    }

    /// <summary>A roll target with a corrupt header is replaced instead of failing the roll.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RollReplacesCorruptTargetHeader(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);

        // Header-sized garbage: length checks pass, header validation throws inside
        // the provisioner, which must fall back to replacing the target.
        await File.WriteAllBytesAsync(SegmentPath(Dir, 2), new byte[JournalFraming.FileHeaderSize], cancellationToken);

        await journal.AppendPutAsync(overflowKey, overflowPayload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        await ledger.WaitUntilValueAsync(ConditionAsync, cancellationToken);

        _ = await Assert.That((await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(ContainsPutKey(Dir, 2, "overflow-key")).IsTrue();
        _ = await Assert.That(journal.HasFlushLoopFailure).IsFalse();
        return;

        static async ValueTask<bool> ConditionAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>A roll replaces a torn pre-created target; only the replacement delta enters the totals.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RollReplacesTornPrecreatedTarget(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);

        // Foreign torn leftover at the roll target (2 bytes, created after startup stats).
        const int tornLength = 2;
        await File.WriteAllBytesAsync(SegmentPath(Dir, 2), ReadOnlyMemory<byte>.Of(0x53, 0x4A), cancellationToken);

        await journal.AppendPutAsync(overflowKey, overflowPayload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        await manifestStore.WaitUntilValueAsync(RolledToTwoAsync, cancellationToken);

        _ = await Assert.That((await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(ContainsPutKey(Dir, 2, "overflow-key")).IsTrue();
        var segmentTwoCopyPath = NodePathKit.Combine(NodePathKit.Combine(Dir, "torn-replaced-reader"), "seg2-copy.jsqx");
        _ = Directory.CreateDirectory(NodePathKit.Combine(Dir, "torn-replaced-reader"));
        File.Copy(SegmentPath(Dir, 2), segmentTwoCopyPath, true);
        var segmentTwo = await File.ReadAllBytesAsync(segmentTwoCopyPath, cancellationToken);
        _ = await Assert.That(segmentTwo.AsSpan(0, 4).SequenceEqual("SJRN"u8)).IsTrue();
        _ = await Assert.That(segmentTwo[4]).IsEqualTo(JournalFraming.Version);
        _ = await Assert.That(Directory.GetFiles(Dir, "*.tmp")).IsEmpty();

        // The foreign torn bytes were never counted (single-writer model counts only writer-caused
        // growth), so the total lags on-disk by exactly those bytes.
        var (_, totalBytes) = JournalReader.GetOnDiskSegmentStats(Dir);
        _ = await Assert.That(pipelined.UsedBytes).IsEqualTo(totalBytes - tornLength);
        return;

        static async ValueTask<bool> RolledToTwoAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>A roll target whose stats cannot be read is treated as missing instead of failing the roll.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public async Task RollSurvivesUnreadableTargetStat(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, cancellationToken);

        var targetPath = SegmentPath(Dir, 2);
        WriteHeaderOnlySegment(targetPath);
        if (!TryMakeUnreadable(targetPath))
            throw new SkipTestException("This environment cannot deny file reads (needs POSIX permissions).");

        await journal.AppendPutAsync(overflowKey, overflowPayload, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);
        await ledger.WaitUntilValueAsync(ConditionAsync, cancellationToken);

        _ = await Assert.That((await ledger.ReadCurrentOrDefaultAsync(cancellationToken)).CurrentJournal).IsEqualTo(2);
        _ = await Assert.That(ContainsPutKey(Dir, 2, "overflow-key")).IsTrue();
        _ = await Assert.That(journal.HasFlushLoopFailure).IsFalse();
        return;

        static async ValueTask<bool> ConditionAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
    }

    /// <summary>Startup creates a missing data directory instead of failing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartupCreatesMissingDataDir(CancellationToken cancellationToken)
    {
        var dataDir = NodePathKit.Combine(Dir, "auto-created");
        var options = CreateOptions(dataDir);
        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            new State { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = null },
            manifestStore,
            new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        await journal.AppendPutAsync(CacheKey.Default("k"), new byte[] { 1, 2, 3 }, cancellationToken);
        await journal.AwaitDurabilityCommitAsync(cancellationToken);

        _ = await Assert.That(Directory.Exists(dataDir)).IsTrue();
        _ = await Assert.That(pipelined.CurrentSegmentIndex).IsEqualTo(1);
    }

    /// <summary>A torn pre-created roll target does not fail startup; it is repaired like the active segment.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TornPrecreatedTargetRepairedOnStartup(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var ledger = new Ledger(options);
        var record = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k", "v");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, record);
        await ledger.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 2, LastSnapshot = null }, cancellationToken);

        // Torn roll target: 3 bytes, shorter than a valid header.
        await File.WriteAllBytesAsync(SegmentPath(Dir, 2), ReadOnlyMemory<byte>.Of(0x53, 0x4A, 0x52), cancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(options, await ledger.ReadCurrentOrDefaultAsync(cancellationToken), ledger, new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;
        _ = await Assert.That(journal.NextSequence).IsEqualTo(2UL);
        _ = await Assert.That(pipelined.CurrentSegmentIndex).IsEqualTo(1);

        var repaired = await File.ReadAllBytesAsync(SegmentPath(Dir, 2), cancellationToken);
        _ = await Assert.That(repaired.Length).IsEqualTo(JournalFraming.FileHeaderSize);
        _ = await Assert.That(repaired.AsSpan(0, 4).SequenceEqual("SJRN"u8)).IsTrue();
        _ = await Assert.That(repaired[4]).IsEqualTo(JournalFraming.Version);
    }

    /// <summary>An existing zero-length segment does not fail startup.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ZeroLengthSegmentStartsUp(CancellationToken cancellationToken)
    {
        var options = CreateOptions(Dir);
        using var manifestStore = new Ledger(options);
        await File.WriteAllBytesAsync(SegmentPath(Dir, 1), [], cancellationToken);
        await manifestStore.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 7, LastSnapshot = null }, cancellationToken);

        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var pipelined = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;

        _ = await Assert.That(journal.NextSequence).IsEqualTo(7UL);
        _ = await Assert.That(pipelined.CurrentSegmentIndex).IsEqualTo(1);
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

        _ = await Assert.That(journal.CurrentSegmentIndex).IsEqualTo(1);
        _ = await Assert.That(journal.ActiveSegmentWrittenBytes + overflowFrameLen > maxBytes).IsTrue();
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

    private static bool TryMakeUnreadable(string path)
    {
        // POSIX denies the stat itself while the directory stays writable for the replace.
        // The probe below also guards elevated environments (root) where permissions
        // are not enforced: without a failing stat the test would prove nothing.
        if (OperatingSystem.IsWindows())
            return false;

        File.SetUnixFileMode(path, UnixFileMode.None);
        try
        {
            _ = new FileInfo(path).Length;
            return false;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return true;
        }
    }

    private static void WriteHeaderOnlySegment(string path)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        RandomAccess.Write(handle, header, 0);
    }
}
