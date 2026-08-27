using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Journal-only recovery must replay from the first on-disk segment, not manifest CurrentJournal.</summary>
[Immutable]
public sealed class ServiceJournalOnlyReplayTests : ServerUnitTestBase
{
    /// <summary>After a segment roll, keys in the closed segment are still required for cache rebuild when no snapshot exists.</summary>
    [Fact]
    public async Task RecoveryReplaysClosedCurrentJournal()
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-journal-only-roll");
        var seg1A = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "seg1-a", "a");
        var seg1B = BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "seg1-b", "b");
        var seg2C = BinaryJournalTestSegmentWriter.BuildPutRecord(3UL, "seg2-c", "c");
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 1, [seg1A, seg1B]);
        BinaryJournalTestSegmentWriter.WriteJournalSegment(scenario.DataDir, 2, seg2C);
        await scenario.Ledger.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 4,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        var gate = new AsyncManualResetEvent(true);
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, gate, new RpcMutationIdempotencyStore(), StoreFactory.CreateReader(persistence)));
        await recovery.StartAsync(DefaultCancellationToken);

        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg1-a"), DefaultCancellationToken)).Found);
        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg1-b"), DefaultCancellationToken)).Found);
        Assert.True((await scenario.Cache.GetValueAsync(CacheKey.Default("seg2-c"), DefaultCancellationToken)).Found);
    }
}
