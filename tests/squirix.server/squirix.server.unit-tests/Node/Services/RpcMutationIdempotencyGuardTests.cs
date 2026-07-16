using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Coordinator and journal integration for durable idempotency.</summary>
public sealed class RpcMutationIdempotencyGuardTests : ServerUnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Execute with a journal must append an IdempotencyOutcome frame.</summary>
    [Fact]
    public async Task CoordinatorWithJournalPersistsOutcomeOnExecute()
    {
        using var dir = new TempDirectory("squirix-coordinator-journal-outcome");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        var store = new RpcMutationIdempotencyStore();
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var key = CacheKey.Default("guard-key");
        var payload = JournalEntryPayloadKit.EncodePut("v");
        var executor = new DurableMutationExecutor(journal);

        _ = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fingerprint",
            (Executor: executor, Journal: journal, Key: key, Payload: payload),
            static async (state, cancellationToken) =>
            {
                var added = await state.Executor.ExecuteAsync(
                    null,
                    static _ => new ValueTask<DurableMutationCondition<bool>>(DurableMutationCondition<bool>.Apply()),
                    new DurableMutationPipeline<IJournalCoordinator, (CacheKey Key, byte[] Payload), byte, bool>(
                        state.Journal,
                        (state.Key, state.Payload),
                        static (j, append, ct) => j.AppendPutAsync(append.Key, append.Payload, ct),
                        0,
                        static (_, _, _) => new ValueTask<bool>(true)),
                    cancellationToken).ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = added };
            },
            DefaultCancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        var found = false;
        using var records = JournalReadPath.ReadAll(options.DataDir, manifest.CurrentJournal, DefaultCancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation is not JournalOperationKind.IdempotencyOutcome)
                continue;

            Assert.Equal(ValidOperationId, record.IdempotencyOperationId);
            found = true;
        }

        Assert.True(found);
    }
}
