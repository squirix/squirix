using System.Collections.Generic;
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
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>
/// A frame the journal thread rejects for on-disk capacity is never written, so its caller must see the definite capacity failure and
/// memory must not apply it: a following durability flush succeeds and would otherwise report the dropped frame as durable.
/// </summary>
/// <remarks>
/// Group commit delivers the rejection through the append's write ack. A plain append has no write ack and returns once its frame is on
/// the ring, so the rejection is dropped silently: the plain cases stay skipped until #703 decides how such a frame is refused.
/// </remarks>
[Immutable]
public sealed class DurableMutationCapacityTests : IsolatedStorageTestBase
{
    private const int CapacityMb = 1;

    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private readonly Meter _testMeter = new("test");

    /// <summary>
    /// A put larger than the remaining journal capacity fails with the capacity error, leaves memory and the journal without it and no
    /// in-flight apply behind, and the next put that fits is applied and replayed.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, Skip = "Fails until #703")]
    [Arguments(true)]
    public async Task CapacityDropFailsCaller(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, CapacityMb, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal);

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(memory.PutAsync(executor, journal.Journal, "big", OversizedValue(), cancellationToken));
        var droppedMemory = memory.Snapshot;
        var pendingApply = journal.Journal.InFlightApplyGate.HasPending;
        _ = await memory.PutAsync(executor, journal.Journal, "a", cancellationToken);
        await journal.ShutdownAsync();
        var replayed = journal.Recover(string.Empty, 0, cancellationToken);

        _ = await Assert.That(droppedMemory).IsEmpty();
        _ = await Assert.That(pendingApply).IsFalse();
        _ = await Assert.That(memory.Snapshot).IsEqualTo(KeyA);
        _ = await Assert.That(replayed).IsEqualTo(KeyA);
    }

    /// <summary>
    /// An idempotent RPC applies memory right after the append, before durability: a rejected frame must fail the handler before that
    /// apply, so neither memory nor the journal holds the mutation and no outcome is recorded for it.
    /// </summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, Skip = "Fails until #703")]
    [Arguments(true)]
    public async Task IdempotentCapacityDropFails(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, CapacityMb, cancellationToken);
        var memory = new AppliedKeys();
        var executor = new DurableMutationExecutor(journal.Journal);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal.Journal);

        var attempts = new int[1];

        _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(ExecuteOversizedAsync(coordinator, executor, journal.Journal, memory, attempts, cancellationToken));

        // Accepted conservative side effect (#702): the mutation is stamped at the ring enqueue, before the write ack delivers the
        // rejection, so the intent stays started. A retry with the same identity answers unknown without running the handler again, until
        // the record ages out or a restart rebuilds it; the outcome is never replayed as a success.
        var retryError = await NodeAsyncAssert.ThrowsAsync<RpcException>(ExecuteOversizedAsync(coordinator, executor, journal.Journal, memory, attempts, cancellationToken));
        await journal.ShutdownAsync();

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retryError.Status.Detail)).IsTrue();
        _ = await Assert.That(attempts[0]).IsEqualTo(1);
        _ = await Assert.That(memory.Snapshot).IsEmpty();
        _ = await Assert.That(ReadOperations(cancellationToken)).IsEmpty();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    /// <summary>Runs one idempotent put of an oversized value under the fixed operation identity and counts each handler invocation.</summary>
    /// <param name="coordinator">Idempotency coordinator under test.</param>
    /// <param name="executor">Durable mutation executor.</param>
    /// <param name="journal">Journal the put is appended to.</param>
    /// <param name="memory">Memory the put applies to.</param>
    /// <param name="attempts">Single-element counter incremented on every handler invocation.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The response of the idempotent call.</returns>
    private static Task<TryAddAsyncResponse> ExecuteOversizedAsync(
        RpcMutationIdempotencyCoordinator coordinator,
        DurableMutationExecutor executor,
        IJournalCoordinator journal,
        AppliedKeys memory,
        int[] attempts,
        CancellationToken cancellationToken) => coordinator.ExecuteAsync(
        OperationId,
        "fingerprint",
        (Executor: executor, Journal: journal, Memory: memory, Attempts: attempts),
        static async (s, ct) =>
        {
            _ = Interlocked.Increment(ref s.Attempts[0]);
            return new TryAddAsyncResponse { Added = await s.Memory.PutAsync(s.Executor, s.Journal, "big", OversizedValue(), ct).ConfigureAwait(false) == 1 };
        },
        cancellationToken);

    /// <summary>Gets a value whose put frame exceeds the whole journal capacity.</summary>
    /// <returns>The oversized value.</returns>
    private static string OversizedValue() => new('x', (CapacityMb * 1024 * 1024) + 1024);

    private List<JournalOperationKind> ReadOperations(CancellationToken cancellationToken)
    {
        var operations = new List<JournalOperationKind>();
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
            operations.Add(records.Current.Operation);

        return operations;
    }
}
