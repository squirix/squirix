using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Journal-only recovery must replay from the first on-disk segment, not manifest CurrentJournal.</summary>
public sealed class RecoveryServiceJournalOnlyReplayTests : UnitTestBase
{
    /// <summary>After a segment roll, keys in the closed segment are still required for cache rebuild when no snapshot exists.</summary>
    [Fact]
    public async Task JournalOnlyRecoveryReplaysClosedSegmentBelowManifestCurrentJournal()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-journal-only-roll");
        var seg1A = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "seg1-a", "a");
        var seg1B = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(2UL, "seg1-b", "b");
        var seg2C = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(3UL, "seg2-c", "c");
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(scenario.DataDir, 1, [seg1A, seg1B]);
        await BinaryJournalTestSegmentWriter.WriteJournalSegmentAsync(scenario.DataDir, 2, [seg2C]);
        await scenario.ManifestStore.WriteAsync(
            new Storage.Manifest.ManifestState
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 4,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        var gate = new JournalStartupGate(false);
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        var recovery = new RecoveryService<object?>(
            persistence,
            scenario.ManifestStore,
            scenario.Cache,
            new RecoveryOptions { BlockOnStart = true },
            gate,
            new RpcMutationIdempotencyStore(),
            SnapshotStoreFactory.CreateReader(persistence),
            NullLogger<RecoveryService<object?>>.Instance);
        await recovery.StartAsync(DefaultCancellationToken);

        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg1-a"), DefaultCancellationToken)).Found);
        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg1-b"), DefaultCancellationToken)).Found);
        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg2-c"), DefaultCancellationToken)).Found);
    }
}
