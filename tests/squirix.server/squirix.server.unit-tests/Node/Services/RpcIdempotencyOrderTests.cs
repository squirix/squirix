using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Idempotent durable mutations make their mutation frame durable before applying it, then append the outcome frame and make it durable.</summary>
[Immutable]
public sealed class RpcIdempotencyOrderTests : IsolatedStorageTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private readonly Meter _testMeter = new("test");

    private enum OrderingStep
    {
        Put = 1,
        MutationDurabilityCommit = 2,
        IdempotencyOutcome = 3,
        OutcomeDurabilityCommit = 4,
    }

    /// <summary>The put is made durable before the apply, then the outcome is appended and made durable, for idempotent RPCs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MutationCommitsBeforeOutcomeCommits(CancellationToken cancellationToken)
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = TimeSpan.Zero,
        };

        using var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance);
        await using var inner = JournalCoordinatorFactory.Create(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            NullLoggerFactory.Instance,
            TimeProvider.System,
            out _);

        var trace = new OrderingTrace();
        await using var orderingJournal = new OrderingJournal(inner, trace);
        IJournalCoordinator journal = orderingJournal;
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        var executor = new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance);
        var key = CacheKey.Default("durability-order-key");
        var payload = JournalEntryPayloadKit.EncodePut("v");

        _ = await coordinator.ExecuteAsync(
            OperationId,
            "fingerprint",
            (Executor: executor, Journal: journal, Key: key, Payload: payload),
            static async (state, cancellationToken) =>
            {
                var added = await state.Executor.ExecuteAsync(
                    state.Key,
                    static (_, _) => new ValueTask<DurableMutationCondition<bool>>(DurableMutationCondition<bool>.Apply()),
                    new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload), bool>(
                        (state.Journal, state.Key, state.Payload),
                        static (s, ownership, ct) => s.Journal.AppendPutAsync(ownership, s.Key, s.Payload, ct),
                        static (_, _) => new ValueTask<bool>(true)),
                    cancellationToken).ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = added };
            },
            cancellationToken);

        await trace.AssertExpectedAsync();
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

        _ = await Assert.That(sawPut).IsTrue();
        _ = await Assert.That(sawIdempotency).IsTrue();
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

        public long HighWaterBytes => _inner.HighWaterBytes;

        public QuiescenceGate InFlightApplyGate => _inner.InFlightApplyGate;

        public bool IsJournalGroupCommitEnabled => _inner.IsJournalGroupCommitEnabled;

        public long MaxBytes => _inner.MaxBytes;

        public ulong NextSequence => _inner.NextSequence;

        public double RecentAppendLatencyMs => _inner.RecentAppendLatencyMs;

        public long UsedBytes => _inner.UsedBytes;

        public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, Action? appended, CancellationToken cancellationToken)
        {
            _trace.Record(OrderingStep.IdempotencyOutcome);
            return _inner.AppendIdempotencyOutcomeAsync(operationId, fingerprint, responseBytes, appended, cancellationToken);
        }

        public ValueTask AppendPutAsync(AsyncLockOwnership ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
        {
            _trace.Record(OrderingStep.Put);
            return _inner.AppendPutAsync(ownership, key, entryBytes, cancellationToken);
        }

        public ValueTask AppendRemoveAsync(AsyncLockOwnership ownership, CacheKey key, CancellationToken cancellationToken) => _inner.AppendRemoveAsync(ownership, key, cancellationToken);

        public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
        {
            _trace.RecordDurabilityCommit();
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

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
            _inner.ExecuteUnderSnapshotBarrierAsync(action, cancellationToken);

        public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
            TState state,
            Func<TState, AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
            CancellationToken cancellationToken) => _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken);

        public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> action, CancellationToken cancellationToken) =>
            _inner.ExecuteUnderSnapshotBarrierAsync(state, action, cancellationToken);

        public void FailJournalPipeline(Exception reason) => _inner.FailJournalPipeline(reason);

        public Exception? GetJournalThreadFailure() => _inner.GetJournalThreadFailure();

        public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => _inner.WaitForStartupAsync(cancellationToken);
    }

    private sealed class OrderingTrace
    {
        private byte _count;
        private bool _outcomeAppended;
        private OrderingStep _step0;
        private OrderingStep _step1;
        private OrderingStep _step2;
        private OrderingStep _step3;

        internal async Task AssertExpectedAsync()
        {
            const byte expectedCount = 4;
            _ = await Assert.That(_count).IsEqualTo(expectedCount);
            _ = await Assert.That(_step0).IsEqualTo(OrderingStep.Put);
            _ = await Assert.That(_step1).IsEqualTo(OrderingStep.MutationDurabilityCommit);
            _ = await Assert.That(_step2).IsEqualTo(OrderingStep.IdempotencyOutcome);
            _ = await Assert.That(_step3).IsEqualTo(OrderingStep.OutcomeDurabilityCommit);
        }

        internal void RecordDurabilityCommit() => Record(_outcomeAppended ? OrderingStep.OutcomeDurabilityCommit : OrderingStep.MutationDurabilityCommit);

        internal void Record(OrderingStep step)
        {
            if (step == OrderingStep.IdempotencyOutcome)
                _outcomeAppended = true;

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
                case 3:
                    _step3 = step;
                    return;
                default:
                    throw new InvalidOperationException("Unexpected ordering step.");
            }
        }
    }
}
