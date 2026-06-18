using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Persistence-layer repro for snapshot retention + compaction recovery (no gRPC).</summary>
public sealed class JournalHighVolumeSnapshotRecoveryTests : ServerUnitTestBase
{
    private const int PayloadBytes = 4 * 1024;
    private const int KeysPerBatch = 400;
    private const int BatchCount = 3;
    private const int SampleOrdinal = 24;

    /// <summary>Stride-sampled probe key must survive snapshot retention, compaction, and full replay.</summary>
    [Fact]
    public async Task StrideSampleSurvivesSnapshotRetentionCompactionAndRecovery()
    {
        using var dir = new TempDirectory("squirix-journal-probe-recovery");
        var persistence = JournalCompactionTestSupport.NewPersistence(dir, groupCommitMs: 2);
        using var manifestStore = new ManifestStore(persistence);
        var snapWriter = new SnapshotWriter(dir);

        for (var batch = 0; batch < BatchCount; batch++)
        {
            await JournalCompactionTestSupport.WriteKeyBatchAsync(
                persistence,
                manifestStore,
                batch * KeysPerBatch,
                KeysPerBatch,
                PayloadBytes,
                DefaultCancellationToken);
            await JournalCompactionTestSupport.TakeSnapshotAsync(persistence, manifestStore, snapWriter, batch + 1, DefaultCancellationToken);
        }

        const int keyCount = BatchCount * KeysPerBatch;
        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        await using var recovered = new PhysicalCache<object?>();
        var gate = new JournalStartupGate(false);
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                manifestStore,
                recovered,
                gate,
                new RpcMutationIdempotencyStore(),
                StoreFactory.CreateReader(persistence)));
        await recovery.StartAsync(DefaultCancellationToken);

        var probeIndex = JournalVolumeRecoveryProbes.SampleIndex(SampleOrdinal, keyCount);
        var key = new CacheKey(JournalCompactionTestSupport.VolumeNamespace, JournalCompactionTestSupport.FormatKey(probeIndex));
        var expected = JournalCompactionTestSupport.CreatePayload(probeIndex, PayloadBytes);
        var result = await recovered.GetValueAsync(key, DefaultCancellationToken);
        if (result is { Found: true, Value: byte[] bytes } && bytes.AsSpan().SequenceEqual(expected))
            return;

        var report = await JournalVolumeRecoveryDiagnostics.BuildReportAsync(
            dir,
            JournalCompactionTestSupport.VolumeNamespace,
            JournalCompactionTestSupport.FormatKey(probeIndex),
            DefaultCancellationToken);
        Assert.Fail($"recovery lost {key.Key} (sampleOrdinal={SampleOrdinal}, index={probeIndex}, keyCount={keyCount})\n{report}");
    }
}
