using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Compaction must preserve durable idempotency journal frames.</summary>
public sealed class JournalCompactorIdempotencyTests : UnitTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private const string Fingerprint = "try-add-entry-async|default|compact-key|abc123";

    /// <summary>Compacted journal segments must retain IdempotencyOutcome frames from the pre-compaction tail.</summary>
    [Fact]
    public async Task CompactionPreservesIdempotencyOutcomeFrames()
    {
        using var dir = new TempDirectory("squirix-compact-idempotency-frames");
        var persistence = CreatePersistence(dir.Path);
        using var manifestStore = new ManifestStore(persistence);
        await WritePutAndIdempotencyAsync(persistence, manifestStore);

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        var found = false;
        foreach (var record in JournalReadPath.ReadAll(persistence.DataDir, manifest.CurrentJournal, DefaultCancellationToken))
        {
            if (record.Operation is not JournalOperationKind.IdempotencyOutcome)
                continue;

            Assert.Equal(OperationId, record.IdempotencyOperationId);
            found = true;
        }

        Assert.True(found);
    }

    /// <summary>Recovery after compaction must restore idempotency replay from the compacted journal.</summary>
    [Fact]
    public async Task CompactionRecoveryRestoresIdempotencyReplay()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-compact-idempotency-recovery");
        var persistence = CreatePersistence(scenario.DataDir);
        await WritePutAndIdempotencyAsync(persistence, scenario.ManifestStore);
        await JournalCompactor.CompactAsync(persistence, scenario.ManifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var idempotencyStore = new RpcMutationIdempotencyStore();
        await RunRecoveryAsync(scenario, persistence, idempotencyStore);

        var replayed = idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out var response);
        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    private static PersistenceOptions CreatePersistence(string dataDir) =>
        new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };

    private static async Task WritePutAndIdempotencyAsync(PersistenceOptions persistence, ManifestStore manifestStore)
    {
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        await journal.AppendPutAsync(
            CacheKey.Default("compact-key"),
            JournalEntryPayloadKit.EncodePut("v"),
            DefaultCancellationToken);
        await journal.AppendIdempotencyOutcomeAsync(
            OperationId,
            Fingerprint,
            RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }),
            DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
    }

    private static Task RunRecoveryAsync(
        RecoveryScenarioBuilder scenario,
        PersistenceOptions persistence,
        RpcMutationIdempotencyStore idempotencyStore)
    {
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                scenario.ManifestStore,
                scenario.Cache,
                new JournalStartupGate(false),
                idempotencyStore,
                StoreFactory.CreateReader(persistence)));
        return recovery.StartAsync(DefaultCancellationToken);
    }
}
