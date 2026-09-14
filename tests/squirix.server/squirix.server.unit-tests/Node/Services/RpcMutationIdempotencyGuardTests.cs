using System;
using System.Diagnostics.Metrics;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Coordinator and journal integration for durable idempotency.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyGuardTests : IsolatedStorageTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>Execute with a journal must append an IdempotencyOutcome frame.</summary>
    [Fact]
    public async Task JournaledCoordinatorPersistsOutcome()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
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
                    new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload), bool>(
                        (state.Journal, state.Key, state.Payload),
                        static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                        static (_, _) => new ValueTask<bool>(true)),
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
            if (record.Operation != JournalOperationKind.IdempotencyOutcome)
                continue;

            Assert.Equal(ValidOperationId, record.IdempotencyOperationId);
            found = true;
        }

        Assert.True(found);
    }

    /// <summary>A failure after the mutation appending keeps the started intent: retry surfaces unknown without re-executing.</summary>
    [Fact]
    public async Task AppendThenThrowSurfacesUnknown()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var attempts = new MutableCount();
        var executor = new DurableMutationExecutor(journal);
        var key = CacheKey.Default("guard-key");
        var payload = JournalEntryPayloadKit.EncodePut("v");

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            coordinator.ExecuteAsync<(DurableMutationExecutor Executor, IJournalCoordinator Journal, CacheKey Key, byte[] Payload, MutableCount Attempts), TryAddAsyncResponse>(
                ValidOperationId,
                "fingerprint",
                (Executor: executor, Journal: journal, Key: key, Payload: payload, Attempts: attempts),
                static async (state, cancellationToken) =>
                {
                    state.Attempts.Value++;
                    _ = await state.Executor.ExecuteAsync(
                        null,
                        static _ => new ValueTask<DurableMutationCondition<bool>>(DurableMutationCondition<bool>.Apply()),
                        new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload), bool>(
                            (state.Journal, state.Key, state.Payload),
                            static (s, ct) => s.Journal.AppendPutAndAwaitDurabilityAsync(s.Key, s.Payload, ct),
                            static (_, _) => new ValueTask<bool>(true)),
                        cancellationToken).ConfigureAwait(false);
                    throw new InvalidOperationException("boom");
                },
                DefaultCancellationToken));

        Assert.Equal(1, attempts.Value);

        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            coordinator.ExecuteAsync(
                ValidOperationId,
                "fingerprint",
                attempts,
                static (state, _) =>
                {
                    state.Value++;
                    return Task.FromResult(new TryAddAsyncResponse { Added = false });
                },
                DefaultCancellationToken));

        Assert.Equal(StatusCode.Unavailable, error.StatusCode);
        Assert.True(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail));
        Assert.Equal(1, attempts.Value);
    }

    /// <summary>A failure before any appending releases the intent so a retry re-executes.</summary>
    [Fact]
    public async Task ThrowBeforeAppendRetriesCleanly()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new Ledger(options);
        await using var journal = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var attempts = new MutableCount();

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(
            coordinator.ExecuteAsync(
                ValidOperationId,
                "fingerprint",
                attempts,
                static (state, _) =>
                {
                    state.Value++;
                    return Task.FromException<TryAddAsyncResponse>(new InvalidOperationException("boom"));
                },
                DefaultCancellationToken));

        var response = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fingerprint",
            attempts,
            static (state, _) =>
            {
                state.Value++;
                return Task.FromResult(new TryAddAsyncResponse { Added = true });
            },
            DefaultCancellationToken);

        Assert.True(response.Added);
        Assert.Equal(2, attempts.Value);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private sealed class MutableCount
    {
        internal int Value { get; set; }
    }
}
