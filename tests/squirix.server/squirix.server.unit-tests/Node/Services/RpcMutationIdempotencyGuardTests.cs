using System;
using System.Diagnostics.Metrics;
using System.Threading;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Coordinator and journal integration for durable idempotency.</summary>
[Immutable]
public sealed class RpcMutationIdempotencyGuardTests : IsolatedStorageTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    /// <summary>A failure after the mutation appending keeps the started intent: retry surfaces unknown without re-executing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendThenThrowSurfacesUnknown(CancellationToken cancellationToken)
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
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
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
                        static (_, _) => new ValueTask<DurableMutationCondition<bool>>(DurableMutationCondition<bool>.Apply()),
                        new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload), bool>(
                            (state.Journal, state.Key, state.Payload),
                            static (s, ct) => s.Journal.AppendPutAndAwaitDurabilityAsync(s.Key, s.Payload, ct),
                            static (_, _) => new ValueTask<bool>(true)),
                        cancellationToken).ConfigureAwait(false);
                    throw new InvalidOperationException("boom");
                },
                cancellationToken));

        _ = await Assert.That(attempts.Value).IsEqualTo(1);

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
                cancellationToken));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(attempts.Value).IsEqualTo(1);
    }

    /// <summary>Execute with a journal must append an IdempotencyOutcome frame.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournaledCoordinatorPersistsOutcome(CancellationToken cancellationToken)
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
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
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
                    static (_, _) => new ValueTask<DurableMutationCondition<bool>>(DurableMutationCondition<bool>.Apply()),
                    new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload), bool>(
                        (state.Journal, state.Key, state.Payload),
                        static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                        static (_, _) => new ValueTask<bool>(true)),
                    cancellationToken).ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = added };
            },
            cancellationToken);

        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
        var found = false;
        using var records = JournalReadPath.ReadAll(options.DataDir, manifest.CurrentJournal, cancellationToken);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation != JournalOperationKind.IdempotencyOutcome)
                continue;

            _ = await Assert.That(record.IdempotencyOperationId).IsEqualTo(ValidOperationId);
            found = true;
        }

        _ = await Assert.That(found).IsTrue();
    }

    /// <summary>A failure before any appending releases the intent so a retry re-executes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowBeforeAppendRetriesCleanly(CancellationToken cancellationToken)
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
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
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
                cancellationToken));

        var response = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fingerprint",
            attempts,
            static (state, _) =>
            {
                state.Value++;
                return Task.FromResult(new TryAddAsyncResponse { Added = true });
            },
            cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(attempts.Value).IsEqualTo(2);
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
