using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A frame that would exceed the on-disk journal capacity is never written, so an idempotent RPC must fail before its memory apply and
/// record no outcome for it.
/// </summary>
/// <remarks>
/// Append admission refuses such a frame before it enters the ring, in plain and group commit mode alike: a plain append returns
/// once its frame is on the ring and has no write ack that could carry a later rejection by the journal thread.
/// </remarks>
[Immutable]
public sealed class RpcMutationIdempotencyCapacityTests : IsolatedStorageTestBase
{
    private const int CapacityMb = 1;

    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private static readonly string KeyBig = CacheKey.Default("big").ToString();

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// An idempotent RPC applies memory right after the append, before durability: a rejected frame must fail the handler before that
    /// apply, so neither memory nor the journal holds the mutation and no outcome is recorded for it. The frame is refused before it is
    /// enqueued, so nothing is stamped: the intent is released, and a retry with the same identity re-executes and gets the same definite
    /// failure instead of an unknown outcome.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IdempotentCapacityDropFails(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, CapacityMb, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal.Journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);

        var attempts = new int[1];

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(ExecuteBigPutAsync(coordinator, executor, journal.Journal, memory, attempts, OversizedValue(), cancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(ExecuteBigPutAsync(coordinator, executor, journal.Journal, memory, attempts, OversizedValue(), cancellationToken));
        await journal.ShutdownAsync();

        _ = await Assert.That(attempts[0]).IsEqualTo(2);
        _ = await Assert.That(memory.Snapshot).IsEmpty();
        _ = await Assert.That(ReadOperations(cancellationToken)).IsEmpty();
    }

    /// <summary>
    /// An idempotent put whose mutation frame is admitted, but whose outcome frame no longer fits: the outcome frame is refused at admission
    /// after the mutation frame may already be durable, so the caller and a retry get the unknown outcome and the handler does not run
    /// again. Pins the current behavior: memory holds the mutation, and the journal holds its frame without an outcome.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IdempotentOutcomeRefusalIsUnknown(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, CapacityMb, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal.Journal, NullLogger<RpcMutationIdempotencyCoordinator>.Instance);
        var attempts = new int[1];
        var value = ValueFillingJournal(8);

        var first = await NodeAsyncAssert.ThrowsAsync<RpcException>(ExecuteBigPutAsync(coordinator, executor, journal.Journal, memory, attempts, value, cancellationToken));
        var retry = await NodeAsyncAssert.ThrowsAsync<RpcException>(ExecuteBigPutAsync(coordinator, executor, journal.Journal, memory, attempts, value, cancellationToken));
        var failed = journal.Journal.GetJournalThreadFailure() != null;
        await journal.ShutdownAsync();

        _ = await Assert.That(failed).IsFalse();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(first.Status.Detail)).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retry.Status.Detail)).IsTrue();
        _ = await Assert.That(attempts[0]).IsEqualTo(1);
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyBig);
        var operation = await Assert.That(ReadOperations(cancellationToken)).HasSingleItem();
        _ = await Assert.That(operation).IsEqualTo(JournalOperationKind.Put);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    /// <summary>Runs one idempotent put of <paramref name="value" /> under key "big" and the fixed operation identity, counting each handler invocation.</summary>
    /// <param name="coordinator">Idempotency coordinator under test.</param>
    /// <param name="executor">Durable mutation executor.</param>
    /// <param name="journal">Journal the put is appended to.</param>
    /// <param name="memory">Memory the put applies to.</param>
    /// <param name="attempts">Single-element counter incremented on every handler invocation.</param>
    /// <param name="value">Value to put.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The response of the idempotent call.</returns>
    private static Task<TryAddAsyncResponse> ExecuteBigPutAsync(
        RpcMutationIdempotencyCoordinator coordinator,
        DurableMutationExecutor executor,
        IJournalCoordinator journal,
        AppliedKeys memory,
        int[] attempts,
        string value,
        CancellationToken cancellationToken) => coordinator.ExecuteAsync(
        OperationId,
        "fingerprint",
        (Executor: executor, Journal: journal, Memory: memory, Attempts: attempts, Value: value),
        static async (s, ct) =>
        {
            _ = Interlocked.Increment(ref s.Attempts[0]);
            return new TryAddAsyncResponse { Added = await s.Memory.PutAsync(s.Executor, s.Journal, "big", s.Value, ct).ConfigureAwait(false) == 1 };
        },
        cancellationToken);

    /// <summary>Gets a value whose put frame exceeds the whole journal capacity.</summary>
    /// <returns>The oversized value.</returns>
    private static string OversizedValue() => new('x', (CapacityMb * 1024 * 1024) + 1024);

    /// <summary>
    /// Gets a value whose idempotent put frame, together with the first segment header, leaves exactly <paramref name="slack" /> bytes of the
    /// journal capacity, too few for the outcome frame that follows it.
    /// </summary>
    /// <param name="slack">Capacity bytes left after the put frame.</param>
    /// <returns>The value.</returns>
    /// <exception cref="InvalidOperationException">No value length produces the exact frame length.</exception>
    private static string ValueFillingJournal(int slack)
    {
        var target = (CapacityMb * 1024 * 1024) - JournalFraming.FileHeaderSize - slack;
        var length = target;
        for (var attempt = 0; attempt < 8; attempt++)
        {
            var value = new string('x', length);
            var record = new JournalRecord
            {
                Sequence = 1,
                UnixMs = 1,
                Operation = JournalOperationKind.Put,
                Key = CacheKey.Default("big"),
                PutEntryBytes = JournalEntryPayloadKit.EncodePut(value),
                MutationOperationId = OperationId,
            };
            var frame = JournalFraming.FrameTotalLength(BinaryJournalCodec.PrepareEncode(record).BodyLength);
            if (frame == target)
                return value;

            length += target - frame;
        }

        throw new InvalidOperationException("no value length fills the journal exactly.");
    }

    private List<JournalOperationKind> ReadOperations(CancellationToken cancellationToken)
    {
        var operations = new List<JournalOperationKind>();
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
            operations.Add(records.Current.Operation);

        return operations;
    }
}
