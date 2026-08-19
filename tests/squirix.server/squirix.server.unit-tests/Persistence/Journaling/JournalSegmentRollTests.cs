using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies journal segment roll happens before the frame that would overflow the active segment.</summary>
[Immutable]
public sealed class JournalSegmentRollTests : ServerUnitTestBase
{
    private const int FillPayloadSize = 8_192;
    private const int LargePayloadSize = 16_000;

    /// <summary>When the next manifest file cannot be created, the roll fails before the overflow frame is appended.</summary>
    [Fact]
    public async Task BlockedNextManifestFileOverflowFrameAppended()
    {
        using var dir = new TempDirectory("squirix-journal-roll-manifest-blocked");
        var options = CreateOptions(dir);
        using var ledger = new Ledger(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            ledger,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        var segmentOnePath = SegmentPath(dir, 1);
        var bytesBefore = new FileInfo(segmentOnePath).Length;

        Exception? rollError = null;
        ledger.EnqueueRoll(1, 1, static () => { }, ex => rollError = ex);
        await StoreTestSupport.WaitUntilAsync(
            ledger,
            static async (l, ct) => (await l.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).NextSequence == 1,
            DefaultCancellationToken);
        StoreTestSupport.ThrowIfFaulted(rollError);

        await File.WriteAllTextAsync(NodePathKit.Combine(dir, StoreTestSupport.ManifestDataFileName(2)), string.Empty, DefaultCancellationToken);
        var block = CountManifestDataFiles(dir);
        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);

        await StoreTestSupport.WaitUntilAsync(pipelined, static j => j.HasFlushLoopFailure, DefaultCancellationToken);
        Assert.True(journal.HasFlushLoopFailure);
        Assert.Equal(bytesBefore, new FileInfo(segmentOnePath).Length);
        Assert.Equal(block, CountManifestDataFiles(dir));
        Assert.False(ContainsPutKey(dir, 1, "overflow-key"));
        Assert.False(ContainsPutKey(dir, 2, "overflow-key"));
    }

    /// <summary>An overflow frame is written only after a successful roll, on the new journal segment file.</summary>
    [Fact]
    public async Task OverflowingAppendLandsOnNextSegmentManifestRoll()
    {
        using var dir = new TempDirectory("squirix-journal-roll-overflow");
        var options = CreateOptions(dir);
        using var manifestStore = new Ledger(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var pipelined = Assert.IsType<JournalCoordinator>(journal);

        var overflowPayload = new byte[LargePayloadSize];
        Array.Fill(overflowPayload, Convert.ToByte('y'));
        var overflowKey = CacheKey.Default("overflow-key");
        var overflowFrameLen = FrameLength(overflowPayload, overflowKey);
        await FillSegmentOneForOverflowAsync(pipelined, overflowFrameLen, DefaultCancellationToken);

        await journal.AppendPutAsync(overflowKey, overflowPayload, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        await StoreTestSupport.WaitUntilAsync(manifestStore, (Func<Ledger, CancellationToken, ValueTask<bool>>)ConditionAsync, DefaultCancellationToken);

        Assert.Equal(2, (await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken)).CurrentJournal);
        Assert.False(ContainsPutKey(dir, 1, "overflow-key"));
        Assert.True(ContainsPutKey(dir, 2, "overflow-key"));
        return;

        static async ValueTask<bool> ConditionAsync(Ledger s, CancellationToken ct)
        {
            return (await s.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false)).CurrentJournal == 2;
        }
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

    private static int CountManifestDataFiles(string dataDir) =>
        Directory.Exists(dataDir) ? Directory.GetFiles(dataDir, $"{FilePrefixes.Manifest}*{FileExtensions.Manifest}").Length : 0;

    private static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 1,
        FlushIntervalMs = 600_000,
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

    private static string SegmentPath(string dataDir, int segmentIndex) => NodePathKit.Combine(
        dataDir,
        $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(segmentIndex)}{FileExtensions.Journal}");
}
