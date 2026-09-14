using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
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

/// <summary>Idempotent durable mutations must append idempotency frames before the durability barrier.</summary>
[Immutable]
public sealed class RpcIdempotencyOrderTests : IsolatedStorageTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    private enum OrderingStep
    {
        Put = 1,
        IdempotencyOutcome = 2,
        AwaitDurabilityCommit = 3,
    }

    /// <summary>Put and IdempotencyOutcome journal appends must precede the durability commit for idempotent RPCs.</summary>
    [Fact]
    public async Task MutationAppendsOutcomeThenCommitsDurably()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 600_000,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
        };

        using var manifestStore = new Ledger(options);
        await using var inner = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        var trace = new OrderingTrace();
        await using var orderingJournal = new OrderingJournal(inner, trace);
        IJournalCoordinator journal = orderingJournal;
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var executor = new DurableMutationExecutor(journal);
        var key = CacheKey.Default("durability-order-key");
        var payload = JournalEntryPayloadKit.EncodePut("v");

        _ = await coordinator.ExecuteAsync(
            OperationId,
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

        trace.AssertExpected();
        await JournalHasPutAndIdempotencyRecordsAsync(options.DataDir, manifestStore);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private static async Task JournalHasPutAndIdempotencyRecordsAsync(string dataDir, Ledger manifestStore)
    {
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None).ConfigureAwait(false);
        var sawPut = false;
        var sawIdempotency = false;
        using var records = JournalReadPath.ReadAll(dataDir, manifest.CurrentJournal, CancellationToken.None);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation is JournalOperationKind.Put)
                sawPut = true;
            if (record.Operation is JournalOperationKind.IdempotencyOutcome)
                sawIdempotency = true;
        }

        Assert.True(sawPut);
        Assert.True(sawIdempotency);
    }

    [Immutable]
    private sealed class OrderingJournal : IJournalCoordinator
    {
        private readonly IJournalCoordinator _inner;
        private readonly OrderingTrace _trace;

        internal OrderingJournal(IJournalCoordinator inner, OrderingTrace trace)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(trace);
            _inner = inner;
            _trace = trace;
        }

        public event EventHandler? OnAppended
        {
            add => _inner.OnAppended += value;
            remove => _inner.OnAppended -= value;
        }

        public long AppendedBytes => _inner.AppendedBytes;

        public long AppendedOps => _inner.AppendedOps;

        public int CurrentSegmentIndex => _inner.CurrentSegmentIndex;

        public bool HasFlushLoopFailure => _inner.HasFlushLoopFailure;

        public long HighWaterBytes => _inner.HighWaterBytes;

        public QuiescenceGate InFlightApplyGate => _inner.InFlightApplyGate;

        public bool IsJournalGroupCommitEnabled => _inner.IsJournalGroupCommitEnabled;

        public long MaxBytes => _inner.MaxBytes;

        public ulong NextSequence => _inner.NextSequence;

        public double RecentAppendLatencyMs => _inner.RecentAppendLatencyMs;

        public long UsedBytes => _inner.UsedBytes;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken)
        {
            _trace.Record(OrderingStep.IdempotencyOutcome);
            return _inner.AppendIdempotencyOutcomeAsync(operationId, fingerprint, responseBytes, cancellationToken);
        }

        public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) =>
            _inner.AppendPutAndAwaitDurabilityAsync(key, entryBytes, cancellationToken);

        public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
        {
            _trace.Record(OrderingStep.Put);
            return _inner.AppendPutAsync(key, entryBytes, cancellationToken);
        }

        public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => _inner.AppendRemoveAsync(key, cancellationToken);

        public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => _inner.AppendRemoveExpirationAsync(key, cancellationToken);

        public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) =>
            _inner.AppendTouchExpirationAsync(key, expiresUtc, cancellationToken);

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
        {
            _trace.Record(OrderingStep.AwaitDurabilityCommit);
            return _inner.AwaitDurabilityCommitAsync(cancellationToken);
        }

        public ValueTask DisposeAsync() => _inner.DisposeAsync();

        public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) =>
            _inner.ExecuteMaintenanceExclusiveAsync(action, cancellationToken);

        public ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
            TState state,
            Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
            Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
            CancellationToken cancellationToken) => _inner.ExecuteSnapshotCutAsync(state, captureUnderBarrier, buildOutsideBarrier, cancellationToken);

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
            _inner.ExecuteUnderSnapshotBarrierAsync(action, cancellationToken);

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken);

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask> action, CancellationToken cancellationToken) =>
            _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken);

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => _inner.WaitForStartupAsync(cancellationToken);
    }

    private sealed class OrderingTrace
    {
        private byte _count;
        private OrderingStep _step0;
        private OrderingStep _step1;
        private OrderingStep _step2;

        internal void AssertExpected()
        {
            Assert.Equal(3, _count);
            Assert.Equal(OrderingStep.Put, _step0);
            Assert.Equal(OrderingStep.IdempotencyOutcome, _step1);
            Assert.Equal(OrderingStep.AwaitDurabilityCommit, _step2);
        }

        internal void Record(OrderingStep step)
        {
            switch (_count++)
            {
                case 0:
                    _step0 = step;
                    return;
                case 1:
                    _step1 = step;
                    return;
                case 2:
                    _step2 = step;
                    return;
                default:
                    throw new InvalidOperationException("Unexpected ordering step.");
            }
        }
    }
}
