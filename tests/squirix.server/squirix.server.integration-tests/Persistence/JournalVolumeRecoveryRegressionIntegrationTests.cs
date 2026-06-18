using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Xunit;

namespace Squirix.Server.IntegrationTests.Persistence;

/// <summary>
/// Fast in-process repro for journal volume recovery loss after snapshot/compaction.
/// Uses the in-process object pipeline, same as other integration tests.
/// </summary>
public sealed class JournalVolumeRecoveryRegressionIntegrationTests : NodeIntegrationTestBase
{
    private const string CacheNamespace = "journal-volume";
    private const int PayloadBytes = 32 * 1024;
    private const int TargetMegabytes = 128;
    private const int SampleCount = 50;

    /// <summary>Fills WAL, restarts, then checks recovery anchors derived from actual keyCount.</summary>
    [Fact]
    public async Task RecoveryAnchorsSurviveRestartAfterJournalVolumeFill()
    {
        var url = GetNextHttpUri();
        const string nodeId = "node_journal_volume_regression";
        var peers = BuildClusterPeers([(nodeId, url)]);
        var snapshotOptions = CreateSnapshotOptions();

        var dataDir = NodePathKit.Combine(
            true,
            NodePathKit.GetProcTempPath(),
            nameof(JournalVolumeRecoveryRegressionIntegrationTests),
            nodeId,
            Guid.NewGuid().ToString("N"));
        DirectoryKit.CreateDirectory(dataDir);

        int keyCount;
        await using (var node = await StartNodeAsync(
                         url,
                         peers,
                         new NodeStartOptions
                         {
                             SnapshotOptions = snapshotOptions,
                             PersistenceOptions = CreatePersistenceOptions(dataDir),
                             UsePersistence = true,
                         }))
        {
            var cache = GetCache(node);
            (keyCount, _, _) = await FillToJournalTargetAsync(node.DataDir, cache, DefaultCancellationToken);
            await AssertRecoveryAnchorsAsync(cache, keyCount, "before restart", node.DataDir, DefaultCancellationToken);
        }

        var restartUrl = GetNextHttpUri();
        var restartPeers = BuildClusterPeers([(nodeId, restartUrl)]);

        await using (var restarted = await StartNodeAsync(
                         restartUrl,
                         restartPeers,
                         new NodeStartOptions
                         {
                             SnapshotOptions = snapshotOptions,
                             PersistenceOptions = CreatePersistenceOptions(dataDir),
                             UsePersistence = true,
                             CleanTestDir = false,
                         }))
        {
            var cache = GetCache(restarted);
            await AssertRecoveryAnchorsAsync(cache, keyCount, "after restart", restarted.DataDir, DefaultCancellationToken);
            await AssertSampledKeysAsync(cache, keyCount, restarted.DataDir, DefaultCancellationToken);
        }
    }

    private static PersistenceOptions CreatePersistenceOptions(string dataDir) => new()
    {
        DataDir = dataDir,
        JournalMaxSegmentMb = 1,

        // The fill targets 128 MB of 1 MB segments; the server's default caps (32 segments / 2048 MB)
        // would trip JournalCapacityExceededException on the 33rd roll well before the target, failing
        // the journal I/O thread. Size the caps to hold the full target with headroom.
        JournalMaxSegmentCount = 256,
        JournalMaxTotalBytesMb = 320,
        FlushIntervalMs = 5,
        JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(2),
    };

    private static TriggerOptions CreateSnapshotOptions() => new ServerJsonSerializer().Deserialize<TriggerOptions>(
        """{"snapshotInterval":"00:00:05","minGapBetweenSnapshots":"00:00:01","journalGrowthThrottleBytes":0}""")!;

    private static async Task AssertRecoveryAnchorsAsync(
        ILogicalNamespacedCache<object?> cache,
        int keyCount,
        string phase,
        string dataDir,
        CancellationToken cancellationToken)
    {
        foreach (var index in JournalVolumeRecoveryProbes.AnchorIndices(keyCount))
            await AssertKeyAsync(cache, keyCount, index, phase, dataDir, cancellationToken);
    }

    private static async Task AssertKeyAsync(
        ILogicalNamespacedCache<object?> cache,
        int keyCount,
        int keyIndex,
        string phase,
        string dataDir,
        CancellationToken cancellationToken)
    {
        var key = FormatKey(keyIndex);
        var expected = CreatePayload(keyIndex);
        var result = await cache.GetValueAsync(CacheNamespace, key, cancellationToken);
        if (result is { Found: true, Value: byte[] bytes } && bytes.AsSpan().SequenceEqual(expected))
            return;

        var report = await JournalVolumeRecoveryDiagnostics.BuildReportAsync(dataDir, CacheNamespace, key, cancellationToken);
        Assert.Fail($"{phase}: missing or corrupt {key} (index={keyIndex}, keyCount={keyCount})\n{report}");
    }

    private static async Task AssertSampledKeysAsync(
        ILogicalNamespacedCache<object?> cache,
        int keyCount,
        string dataDir,
        CancellationToken cancellationToken)
    {
        var sampleCount = Math.Min(SampleCount, keyCount);
        for (var i = 0; i < sampleCount; i++)
        {
            var index = JournalVolumeRecoveryProbes.SampleIndex(i, keyCount);
            await AssertKeyAsync(cache, keyCount, index, $"sample {i}", dataDir, cancellationToken);
        }
    }

    private static async Task<(int KeyCount, int PeakSegments, long PeakJournalBytes)> FillToJournalTargetAsync(
        string dataDir,
        ILogicalNamespacedCache<object?> cache,
        CancellationToken cancellationToken)
    {
        const long targetBytes = TargetMegabytes * 1024L * 1024L;
        var index = 0;
        var peakSegments = 0;
        var peakJournalBytes = 0L;
        while (peakJournalBytes < targetBytes)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await cache.SetEntryAsync(IntegrationMutationOpIds.Default, CacheNamespace, FormatKey(index), BuildEntry(CreatePayload(index)), cancellationToken);
            index++;

            var journalBytes = JournalStorageProbe.GetTotalJournalBytes(dataDir);
            if (journalBytes > peakJournalBytes)
                peakJournalBytes = journalBytes;

            var segments = JournalStorageProbe.CountJournalSegments(dataDir);
            if (segments > peakSegments)
                peakSegments = segments;
        }

        return (index, peakSegments, peakJournalBytes);
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("ZeroAlloc", "ZA0302:Large array allocation in method scope", Justification = "Test payload is the owned result returned to callers; pooling is not applicable.")]
    private static byte[] CreatePayload(int keyIndex)
    {
        var payload = new byte[PayloadBytes];
        BinaryPrimitives.WriteInt32LittleEndian(payload, keyIndex);
        for (var i = 4; i < payload.Length; i++)
            payload[i] = Convert.ToByte((keyIndex + i) % 256);

        return payload;
    }

    private static string FormatKey(int index) => $"vol:{index.ToString(CultureInfo.InvariantCulture)}";
}
