using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Recovery replay of durable idempotency journal frames.</summary>
public sealed class ServiceIdempotencyReplayTests : ServerUnitTestBase
{
    private const string Fingerprint = "try-add-entry-async|default|idempotency-key|abc123";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Journal replay must restore idempotency CreatedUtc from the frame UnixMs, not recovery wall clock.</summary>
    [Fact]
    public async Task JournalReplayRestoresIdempotencyCreatedUtcUnixMs()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-recovery-idempotency-unixms");
        var persistence = CreatePersistence(scenario.DataDir);
        await WritePutAndIdempotencyAsync(scenario, persistence);

        var journalUnixMs = ReadIdempotencyOutcomeUnixMs(scenario.DataDir);

        var idempotencyStore = new RpcMutationIdempotencyStore();
        await RunRecoveryAsync(scenario, persistence, idempotencyStore);

        IIdempotencySnapshotExporter exporter = idempotencyStore;
        var snapshot = new List<PersistedIdempotencyRecord>();
        exporter.ExportSnapshot(snapshot, DateTime.UtcNow);
        var exportedRecord = Assert.Single(snapshot);
        var expectedCreatedUtc = DateTimeOffset.FromUnixTimeMilliseconds(journalUnixMs).UtcDateTime;
        Assert.Equal(OperationId, exportedRecord.OperationId);
        Assert.Equal(expectedCreatedUtc, exportedRecord.CreatedUtc);

        var replayed = idempotencyStore.TryReplay(OperationId, Fingerprint, TryAddAsyncResponse.Parser, out var response);
        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    private static PersistenceOptions CreatePersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };

    private static long ReadIdempotencyOutcomeUnixMs(string dataDir)
    {
        using var records = JournalReadPath.ReadAll(dataDir, 1, CancellationToken.None);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation is JournalOperationKind.IdempotencyOutcome)
                return record.UnixMs;
        }

        throw new InvalidOperationException("IdempotencyOutcome frame was not found in the journal.");
    }

    private static Task RunRecoveryAsync(RecoveryScenarioBuilder scenario, PersistenceOptions persistence, RpcMutationIdempotencyStore idempotencyStore)
    {
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                scenario.Ledger,
                scenario.Cache,
                new JournalStartupGate(false),
                idempotencyStore,
                StoreFactory.CreateReader(persistence)));
        return recovery.StartAsync(DefaultCancellationToken);
    }

    private static async Task WritePutAndIdempotencyAsync(RecoveryScenarioBuilder scenario, PersistenceOptions persistence)
    {
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await scenario.Ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            scenario.Ledger,
            new JournalStartupGate(),
            DefaultCancellationToken);

        await journal.AppendPutAsync(CacheKey.Default("idempotency-key"), JournalEntryPayloadKit.EncodePut("v"), DefaultCancellationToken);
        await journal.AppendIdempotencyOutcomeAsync(
            OperationId,
            Fingerprint,
            RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }),
            DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
    }
}
