using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A hosted idempotent write waits for one journal flush: its outcome frame follows its mutation frame and shares the flush.</summary>
[Immutable]
public sealed class FusedWriteTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(1);

    /// <summary>Every v0.1 mutating RPC waits for exactly one flush, answers what it appended, and replays the same bytes on a retry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryRpcFlushesOnceAndReplaysItsResponse(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var context = new TestServerCallContext();

        var set = FusedWriteHarness.Set(FusedWriteHarness.OpId(1), "a", ttl: Ttl);
        var update = FusedWriteHarness.Update(FusedWriteHarness.OpId(2), "b");
        var touch = FusedWriteHarness.Touch(FusedWriteHarness.OpId(3), TimeSpan.FromMinutes(2));
        var removeExpiration = FusedWriteHarness.RemoveExpiration(FusedWriteHarness.OpId(4));
        var tryAdd = FusedWriteHarness.AddIfAbsent(FusedWriteHarness.OpId(5), "c", "k2");
        var getOrAdd = FusedWriteHarness.GetOrAdd(FusedWriteHarness.OpId(6), "d", "k3");
        var remove = FusedWriteHarness.Remove(FusedWriteHarness.OpId(7));

        _ = await AssertFusedAsync(harness, set.OperationId, () => harness.Adapter.SetEntry(set, context), cancellationToken);
        var updated = await AssertFusedAsync(harness, update.OperationId, () => harness.Adapter.Update(update, context), cancellationToken);
        var touched = await AssertFusedAsync(harness, touch.OperationId, () => harness.Adapter.Touch(touch, context), cancellationToken);
        var expirationRemoved = await AssertFusedAsync(harness, removeExpiration.OperationId, () => harness.Adapter.RemoveExpiration(removeExpiration, context), cancellationToken);
        var added = await AssertFusedAsync(harness, tryAdd.OperationId, () => harness.Adapter.TryAddEntry(tryAdd, context), cancellationToken);
        var gotOrAdded = await AssertFusedAsync(harness, getOrAdd.OperationId, () => harness.Adapter.GetOrAdd(getOrAdd, context), cancellationToken);
        var removed = await AssertFusedAsync(harness, remove.OperationId, () => harness.Adapter.Remove(remove, context), cancellationToken);

        _ = await Assert.That(updated.Updated).IsTrue();
        _ = await Assert.That(touched.Found).IsTrue();
        _ = await Assert.That(expirationRemoved.Found).IsTrue();
        _ = await Assert.That(added.Added).IsTrue();
        _ = await Assert.That(gotOrAdded.Added).IsTrue();
        _ = await Assert.That(gotOrAdded.Value).IsNotNull();
        _ = await Assert.That(removed.Removed).IsTrue();
        _ = await Assert.That(removed.PreviousValue).IsNotNull();
    }

    /// <summary>The outcome frame sits right behind its mutation frame, and a restart replays both: the effect and a replayable outcome.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RestartReplaysEffectAndOutcome(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var request = FusedWriteHarness.Set(FusedWriteHarness.OpId(1), "a");

        var flushesBefore = harness.FlushCount;
        _ = await harness.Adapter.SetEntry(request, new TestServerCallContext());
        var flushes = harness.FlushCount - flushesBefore;
        await journal.ShutdownAsync();
        var frames = harness.ReadFrames(cancellationToken);
        var (memory, store) = await harness.RecoverAsync(cancellationToken);
        var replayed = store.TryReplay(request.OperationId, RpcMutationFingerprints.SetEntry(request.CacheName, request.Key, request.Entry), SetAsyncResponse.Parser, out _);

        _ = await Assert.That(flushes).IsEqualTo(1);
        _ = await Assert.That(frames).Count().IsEqualTo(2);
        _ = await Assert.That(frames[0].Operation).IsEqualTo(JournalOperationKind.Put);
        _ = await Assert.That(frames[0].MutationOperationId).IsEqualTo(request.OperationId);
        _ = await Assert.That(frames[1].Operation).IsEqualTo(JournalOperationKind.IdempotencyOutcome);
        _ = await Assert.That(frames[1].OutcomeOperationId).IsEqualTo(request.OperationId);
        _ = await Assert.That(frames[1].Sequence).IsEqualTo(frames[0].Sequence + 1);
        _ = await Assert.That((await memory.GetValueAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken)).Found).IsTrue();
        _ = await Assert.That(replayed).IsTrue();
    }

    /// <summary>A remove of an absent key still appends its frame and reports that nothing was removed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveOfAbsentKeyFusesNotRemoved(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var request = FusedWriteHarness.Remove(FusedWriteHarness.OpId(1));

        var response = await AssertFusedAsync(harness, request.OperationId, () => harness.Adapter.Remove(request, new TestServerCallContext()), cancellationToken);

        _ = await Assert.That(response.Removed).IsFalse();
        _ = await Assert.That(response.PreviousValue).IsNull();
    }

    /// <summary>An add over a live key appends nothing for its mutation, so it takes the ordinary path and reports that nothing was added.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddOverLiveKeyIsNotFused(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        _ = await harness.Adapter.SetEntry(FusedWriteHarness.Set(FusedWriteHarness.OpId(1), "a"), new TestServerCallContext());
        var request = FusedWriteHarness.AddIfAbsent(FusedWriteHarness.OpId(2), "b");

        var response = await harness.Adapter.TryAddEntry(request, new TestServerCallContext());
        var frames = harness.ReadFrames(cancellationToken);

        _ = await Assert.That(response.Added).IsFalse();
        _ = await Assert.That(frames.FindAll(frame => string.Equals(frame.MutationOperationId, request.OperationId, StringComparison.Ordinal))).IsEmpty();
    }

    /// <summary>An add over a node that expired but is still in memory takes effect, and its fused outcome says so.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddOverExpiredNodeFusesAdded(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        _ = await harness.Adapter.SetEntry(FusedWriteHarness.Set(FusedWriteHarness.OpId(1), "old", ttl: TimeSpan.FromSeconds(1)), new TestServerCallContext());
        harness.Clock.Advance(TimeSpan.FromSeconds(2));
        var request = FusedWriteHarness.AddIfAbsent(FusedWriteHarness.OpId(2), "new");

        var response = await AssertFusedAsync(harness, request.OperationId, () => harness.Adapter.TryAddEntry(request, new TestServerCallContext()), cancellationToken);
        var current = await harness.Physical.GetValueAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(current.Value is JsonElement element && string.Equals(element.GetProperty("__v").GetString(), "new", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>A remove of a node that expired but is still in memory removes nothing, and its fused outcome says so.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoveOfExpiredNodeReportsNotRemoved(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        _ = await harness.Adapter.SetEntry(FusedWriteHarness.Set(FusedWriteHarness.OpId(1), "old", ttl: TimeSpan.FromSeconds(1)), new TestServerCallContext());
        harness.Clock.Advance(TimeSpan.FromSeconds(1));
        var request = FusedWriteHarness.Remove(FusedWriteHarness.OpId(2));

        var response = await AssertFusedAsync(harness, request.OperationId, () => harness.Adapter.Remove(request, new TestServerCallContext()), cancellationToken);

        _ = await Assert.That(response.Removed).IsFalse();
    }

    /// <summary>Runs an RPC twice with one operation id and checks the first run flushed once, the retry replayed it, and the frames carry its bytes.</summary>
    /// <typeparam name="TResponse">Response type of the RPC.</typeparam>
    /// <param name="harness">The write path.</param>
    /// <param name="operationId">The operation id of the RPC.</param>
    /// <param name="rpc">Runs the RPC.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The response of the first run.</returns>
    private static async Task<TResponse> AssertFusedAsync<TResponse>(FusedWriteHarness harness, string operationId, Func<Task<TResponse>> rpc, CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>
    {
        var flushesBefore = harness.FlushCount;
        var first = await rpc();
        var flushes = harness.FlushCount - flushesBefore;
        var retried = await rpc();
        var retryFlushes = harness.FlushCount - flushesBefore - flushes;
        var frames = harness.ReadFrames(cancellationToken);
        var outcome = frames[^1];
        var mutation = frames[^2];

        _ = await Assert.That(flushes).IsEqualTo(1);
        _ = await Assert.That(retryFlushes).IsEqualTo(0);
        _ = await Assert.That(retried).IsEqualTo(first);
        _ = await Assert.That(mutation.MutationOperationId).IsEqualTo(operationId);
        _ = await Assert.That(outcome.Operation).IsEqualTo(JournalOperationKind.IdempotencyOutcome);
        _ = await Assert.That(outcome.OutcomeOperationId).IsEqualTo(operationId);
        _ = await Assert.That(outcome.OutcomeBytes.AsSpan().SequenceEqual(first.ToByteArray())).IsTrue();
        return first;
    }
}
