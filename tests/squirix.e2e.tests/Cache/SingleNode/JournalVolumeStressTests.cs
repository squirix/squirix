using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests;
using Squirix.E2ETests.Support.Restart;
using Squirix.E2ETests.Support.Stress;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Journaling;
using Xunit;

namespace Squirix.E2ETests.Cache.SingleNode;

/// <summary>Sustained durable writes through segment roll, snapshot, compaction, and repeated restart recovery.</summary>
[Trait(Category.TraitName, Category.TraitValue)]
public sealed class JournalVolumeStressTests : LoadTestBase
{
    private const string CacheNamespace = "journal-volume";

    /// <summary>
    /// Appends a configurable journal volume with small segments, then verifies sampled keys survive
    /// multiple node restarts and post-restart writes.
    /// </summary>
    [Fact]
    public async Task HeavyJournalSurvivesRollCompactionAndRepeatedRestart()
    {
        var profile = new LoadProfile(JournalVolumeStressSettings.WriterCount, JournalVolumeStressSettings.Budget);
        using var deadline = CreateDeadline(profile);
        var token = deadline.Token;
        var hostOptions = CreateHostOptions();

        await using var node = await RestartableSingleNode.StartWithOptionsAsync(
            nameof(HeavyJournalSurvivesRollCompactionAndRepeatedRestart),
            hostOptions,
            JournalVolumeStressSettings.RpcPerAttemptTimeout,
            token);

        var cache = await node.GetCacheAsync<byte[]>(CacheNamespace, token);
        var (keyCount, peakSegmentCount, peakJournalBytes) = await FillJournalToTargetAsync(node, cache, token);

        Assert.True(keyCount > 0);
        Assert.True(peakSegmentCount >= 1);
        Assert.True(peakJournalBytes >= JournalVolumeStressSettings.TargetJournalBytes);

        if (JournalVolumeStressSettings.TargetJournalBytes > JournalVolumeStressSettings.SegmentMegabytes * 1024L * 1024L)
            Assert.True(peakSegmentCount >= 2);

        await AssertRecoveryAnchorsAsync(node, cache, keyCount, "before restart", token);

        await node.RestartAsync(token);
        cache = await node.GetCacheAsync<byte[]>(CacheNamespace, token);
        await AssertRecoveryAnchorsAsync(node, cache, keyCount, "after restart", token);
        await AssertSampledKeysAsync(node, cache, keyCount, token);

        for (var restart = 0; restart < 2; restart++)
        {
            var markerKey = $"post-restart:{restart.ToString(CultureInfo.InvariantCulture)}";
            var markerPayload = CreatePayload(-1 - restart);
            await cache.SetAsync(markerKey, markerPayload, cancellationToken: token);
            await node.RestartAsync(token);
            cache = await node.GetCacheAsync<byte[]>(CacheNamespace, token);
            await AssertRecoveryAnchorsAsync(node, cache, keyCount, $"after restart {restart}", token);
            await AssertSampledKeysAsync(node, cache, keyCount, token);

            var marker = await cache.GetValueAsync(markerKey, token);
            Assert.True(marker.Found);
            Assert.Equal(markerPayload, marker.Value);
        }
    }

    private static TestNodeHostStartOptions CreateHostOptions() => new()
    {
        JournalMaxSegmentMb = JournalVolumeStressSettings.SegmentMegabytes,
        JournalMaxSegmentCount = JournalVolumeStressSettings.JournalMaxSegmentCount,
        JournalMaxTotalBytesMb = JournalVolumeStressSettings.JournalMaxTotalBytesMb,
        FlushIntervalMs = 5,
        SnapshotInterval = TimeSpan.FromSeconds(5),
        JournalGroupCommitMaxWaitMs = 2,
    };

    private static async Task AssertRecoveryAnchorsAsync(
        RestartableSingleNode node,
        ICache<byte[]> cache,
        int keyCount,
        string phase,
        CancellationToken token)
    {
        foreach (var index in JournalVolumeRecoveryProbes.AnchorIndices(keyCount))
            await AssertKeyAsync(node, cache, keyCount, index, $"{phase} anchor", token);
    }

    private static async Task AssertKeyAsync(
        RestartableSingleNode node,
        ICache<byte[]> cache,
        int keyCount,
        int keyIndex,
        string phase,
        CancellationToken token)
    {
        var key = FormatKey(keyIndex);
        var expected = CreatePayload(keyIndex);
        var result = await cache.GetValueAsync(key, token);
        if (result is { Found: true, Value: not null } && result.Value.AsSpan().SequenceEqual(expected))
            return;

        var report = await JournalVolumeRecoveryDiagnostics.BuildReportAsync(node.DataDir, CacheNamespace, key, token);
        Assert.Fail($"{phase}: missing or corrupt {key} (index={keyIndex}, keyCount={keyCount})\n{report}");
    }

    private static async Task AssertSampledKeysAsync(RestartableSingleNode node, ICache<byte[]> cache, int keyCount, CancellationToken token)
    {
        var sampleCount = Math.Min(JournalVolumeStressSettings.SampleCount, keyCount);
        for (var i = 0; i < sampleCount; i++)
        {
            var index = JournalVolumeRecoveryProbes.SampleIndex(i, keyCount);
            var key = FormatKey(index);
            var expected = CreatePayload(index);
            var result = await cache.GetValueAsync(key, token);
            if (result is { Found: true, Value: not null } && result.Value.AsSpan().SequenceEqual(expected))
                continue;

            var report = await JournalVolumeRecoveryDiagnostics.BuildReportAsync(node.DataDir, CacheNamespace, key, token);
            Assert.Fail($"sample miss {key} (sampleOrdinal={i}, index={index}, keyCount={keyCount})\n{report}");
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage("ZeroAlloc", "ZA0302:Large array allocation in method scope", Justification = "Test payload is the owned result returned to callers; pooling is not applicable.")]
    private static byte[] CreatePayload(int keyIndex)
    {
        var payload = new byte[JournalVolumeStressSettings.PayloadBytes];
        BinaryPrimitives.WriteInt32LittleEndian(payload, keyIndex);
        for (var i = 4; i < payload.Length; i++)
            payload[i] = Convert.ToByte((keyIndex + i) % 256);

        return payload;
    }

    private static async Task<(int KeyCount, int PeakSegmentCount, long PeakJournalBytes)> FillJournalToTargetAsync(RestartableSingleNode node, ICache<byte[]> cache, CancellationToken token)
    {
        var index = 0;
        var peakSegmentCount = 0;
        var peakJournalBytes = 0L;
        var targetBytes = JournalVolumeStressSettings.TargetJournalBytes;
        while (!token.IsCancellationRequested && peakJournalBytes < targetBytes)
        {
            try
            {
                await cache.SetAsync(FormatKey(index), CreatePayload(index), cancellationToken: token);
            }
            catch (Grpc.Core.RpcException ex)
            {
                var bytesNow = JournalStorageProbe.GetTotalJournalBytes(node.DataDir);
                var segmentsNow = JournalStorageProbe.CountJournalSegments(node.DataDir);
                Assert.Fail(
                    $"SetAsync failed at index={index} (journalBytes={bytesNow}, segmentCount={segmentsNow}, targetBytes={targetBytes}, segmentMb={JournalVolumeStressSettings.SegmentMegabytes}): {ex.Status.Detail}");
            }

            index++;

            var journalBytes = JournalStorageProbe.GetTotalJournalBytes(node.DataDir);
            if (journalBytes > peakJournalBytes)
                peakJournalBytes = journalBytes;

            var segmentCount = JournalStorageProbe.CountJournalSegments(node.DataDir);
            if (segmentCount > peakSegmentCount)
                peakSegmentCount = segmentCount;
        }

        return (index, peakSegmentCount, peakJournalBytes);
    }

    private static string FormatKey(int index) => $"vol:{index.ToString(CultureInfo.InvariantCulture)}";
}
