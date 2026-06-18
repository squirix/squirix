using System;
using System.Threading;
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

/// <summary>Fast persistence-layer repro for keys lost across snapshot, retention, and compaction.</summary>
public sealed class JournalCompactionStateCompletenessTests : ServerUnitTestBase
{
    private const int PayloadBytes = 4 * 1024;
    private const int KeysPerBatch = 400;
    private const int BatchCount = 3;
    private const int SampleCount = 50;

    /// <summary>All written keys must appear in the compacted journal and survive recovery (object? pipeline).</summary>
    [Fact]
    public Task AllWrittenKeysSurviveCompactionRoundTripWithGroupCommit() =>
        RunAndAssertCompactionPipelineAsync(2, DefaultCancellationToken);

    /// <summary>Same pipeline without group commit — isolates group-commit interaction.</summary>
    [Fact]
    public Task AllWrittenKeysSurviveCompactionRoundTripWithoutGroupCommit() =>
        RunAndAssertCompactionPipelineAsync(0, DefaultCancellationToken);

    private static async Task RunAndAssertCompactionPipelineAsync(int groupCommitMs, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-completeness");
        var persistence = JournalCompactionTestSupport.NewPersistence(dir, groupCommitMs: groupCommitMs);
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
                cancellationToken);
            await JournalCompactionTestSupport.TakeSnapshotAsync(persistence, manifestStore, snapWriter, batch + 1, cancellationToken);
        }

        const int keyCount = BatchCount * KeysPerBatch;
        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), cancellationToken);
        await AssertJournalKeyCoverageAsync(dir, keyCount, cancellationToken);

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
        await recovery.StartAsync(cancellationToken);
        await AssertRecoveredSamplesAsync(dir, recovered, keyCount, cancellationToken);
    }

    private static async Task AssertJournalKeyCoverageAsync(string dir, int keyCount, CancellationToken cancellationToken)
    {
        var uniqueKeys = JournalCompactionProbe.CountUniquePutKeys(dir, JournalCompactionTestSupport.VolumeNamespace);
        if (uniqueKeys >= keyCount)
            return;

        var probeIndex = JournalVolumeRecoveryProbes.SampleIndex(24, keyCount);
        var report = await JournalVolumeRecoveryDiagnostics.BuildReportAsync(
            dir,
            JournalCompactionTestSupport.VolumeNamespace,
            JournalCompactionTestSupport.FormatKey(probeIndex),
            cancellationToken);
        Assert.Fail($"compacted journal has {uniqueKeys} keys, expected {keyCount}\n{report}");
    }

    private static async Task AssertRecoveredSamplesAsync(
        string dir,
        PhysicalCache<object?> recovered,
        int keyCount,
        CancellationToken cancellationToken)
    {
        var sampleCount = Math.Min(SampleCount, keyCount);
        for (var i = 0; i < sampleCount; i++)
        {
            var index = JournalVolumeRecoveryProbes.SampleIndex(i, keyCount);
            var key = new CacheKey(JournalCompactionTestSupport.VolumeNamespace, JournalCompactionTestSupport.FormatKey(index));
            var expected = JournalCompactionTestSupport.CreatePayload(index, PayloadBytes);
            var result = await recovered.GetValueAsync(key, cancellationToken);
            if (result is { Found: true, Value: byte[] bytes } && bytes.AsSpan().SequenceEqual(expected))
                continue;

            var report = await JournalVolumeRecoveryDiagnostics.BuildReportAsync(
                dir,
                JournalCompactionTestSupport.VolumeNamespace,
                JournalCompactionTestSupport.FormatKey(index),
                cancellationToken);
            Assert.Fail($"recovery lost {key.Key} (sampleOrdinal={i}, index={index}, keyCount={keyCount})\n{report}");
        }
    }
}
