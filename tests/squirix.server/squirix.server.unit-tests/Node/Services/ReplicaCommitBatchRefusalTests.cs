using System;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitBatchKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>How the writes of a batch are decided when the committer is disposed, the term changes, the budget ends, or the local append is unconfirmed.</summary>
public sealed class ReplicaCommitBatchRefusalTests : IsolatedStorageTestBase
{
    private const string UnknownCommitInstrument = "squirix_replication_unknown_commit_total";

    private static readonly TimeSpan DisposeBudget = TimeSpan.FromMilliseconds(200);

    /// <summary>A dispose that finds the leader stuck on the commit gate refuses every queued write as without a leader, except one a retained entry answers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeRefusesQueuedWrites(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        var committer = CreateCommitter(registry, local, new AcceptingGateway(), DisposeBudget);
        try
        {
            var committed = NewOperationId();
            await SetAsync(committer, "k0", committed, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            var held = await committer.Gate.LockAsync(cancellationToken);
            var leader = SetAsync(committer, "k1", cancellationToken);
            var refused = SetAsync(committer, "k2", cancellationToken);
            var retry = SetAsync(committer, "k0", committed, cancellationToken);
            var second = SetAsync(committer, "k3", cancellationToken);

            await committer.DisposeAsync().AsTask().WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            held.Dispose();

            await retry.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            foreach (var write in new[] { leader, refused, second })
            {
                var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
                _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unavailable);
                _ = await Assert.That(error.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
            }

            _ = await Assert.That(Applied(local)).IsEqualTo("k0");
        }
        finally
        {
            await committer.DisposeAsync();
        }
    }

    /// <summary>
    /// A dispose that refuses the queue while the leader prepares a write it took leaves that write to the leader: the leader commits it once, and the
    /// writes the dispose took are refused once.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeWhilePreparingDecidesOnce(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, new AcceptingGateway(), TimeProvider.System);
        var held = await committer.Gate.LockAsync(cancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var leader = SetAsync(committer, "k1", cancellationToken);
        var preparing = PrepareWriteAsync(committer, "k2", (factory, index, _) => PrepareAfterAsync(factory, "k2", index, entered, proceed), cancellationToken);
        var queued = SetAsync(committer, "k3", cancellationToken);
        held.Dispose();
        await entered.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        committer.RefuseQueued();
        _ = proceed.TrySetResult();

        await Task.WhenAll(leader, preparing).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(queued.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        _ = await Assert.That(refused.Status.Detail).IsEqualTo(ServerOpContract.NoLeaderAuthorityDetail);
        _ = await Assert.That(Applied(local)).IsEqualTo("k1,k2");
        _ = await Assert.That((await GetOwnedLog(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>
    /// A higher term that reaches the log while a batch is formed makes the append of the batch refuse: every write of it fails with its own stale-term
    /// refusal, and nothing is applied.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TermChangeRefusesTheWholeBatch(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, new AcceptingGateway(), TimeProvider.System);
        var held = await committer.Gate.LockAsync(cancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var first = SetAsync(committer, "k1", cancellationToken);
        var second = PrepareWriteAsync(committer, "k2", (factory, index, _) => PrepareAfterAsync(factory, "k2", index, entered, proceed), cancellationToken);
        var third = SetAsync(committer, "k3", cancellationToken);
        held.Dispose();
        await entered.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        _ = await GetOwnedLog(registry).ObserveTermAsync(99UL, cancellationToken);
        _ = proceed.TrySetResult();

        var errors = new[]
        {
            await NodeAsyncAssert.ThrowsAsync<RpcException>(first.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)),
            await NodeAsyncAssert.ThrowsAsync<RpcException>(second.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)),
            await NodeAsyncAssert.ThrowsAsync<RpcException>(third.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken)),
        };
        foreach (var error in errors)
            _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);

        _ = await Assert.That(ReferenceEquals(errors[0], errors[1]) || ReferenceEquals(errors[1], errors[2]) || ReferenceEquals(errors[0], errors[2])).IsFalse();
        _ = await Assert.That(Applied(local)).IsEqualTo(string.Empty);
    }

    /// <summary>
    /// A local append of a batch that fails with its frames possibly on disk drops the started state, reports the fault of the log, and decides each write
    /// on its own from its idempotency record; the unknown outcome is counted once per write.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnconfirmedAppendCountsEachWrite(CancellationToken cancellationToken)
    {
        using var meter = new Meter("squirix-unknown-batch");
        var kinds = new ConcurrentQueue<string>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, target) =>
        {
            if (ReferenceEquals(instrument.Meter, meter) && string.Equals(instrument.Name, UnknownCommitInstrument, StringComparison.Ordinal))
                target.EnableMeasurementEvents(instrument, kinds);
        };
        listener.SetMeasurementEventCallback<long>(static (_, _, tags, state) =>
        {
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, "operation", StringComparison.Ordinal) && state is ConcurrentQueue<string> seen && tag.Value is string operation)
                    seen.Enqueue(operation);
            }
        });
        listener.Start();
        var faults = new StateInstallFaultHooks();
        var local = RecoveredCache();
        await using var registry = await OpenRegistryAsync(new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System, FaultHooks = faults }, cancellationToken);
        await using var committer = CreateCommitter(registry, local, new AcceptingGateway(), (TimeProvider.System, new ReplicationMetrics(meter)));
        var held = await committer.Gate.LockAsync(cancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var proceed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writes = new[]
        {
            SetAsync(committer, "k1", cancellationToken),
            PrepareWriteAsync(committer, "k2", (factory, index, _) => PrepareAfterAsync(factory, "k2", index, entered, proceed), cancellationToken),
            SetAsync(committer, "k3", cancellationToken),
        };
        held.Dispose();
        await entered.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        faults.FailFlush(1);
        _ = proceed.TrySetResult();

        foreach (var write in writes)
        {
            var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
            _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        }

        _ = await Assert.That(committer.IsStarted).IsFalse();
        _ = await Assert.That(committer.AppendProgress().Failed).IsGreaterThan(0UL);
        await SequenceAssert.EqualAsync(["user_mutation", "user_mutation", "user_mutation"], kinds.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A refusal before the append that finds the log holding the first entry is counted as an unknown outcome only when the caller asks for it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreAppendUnknownIsCountedOnlyOnRequest(CancellationToken cancellationToken)
    {
        using var meter = new Meter("squirix-unknown-preappend");
        var count = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, target) =>
        {
            if (ReferenceEquals(instrument.Meter, meter) && string.Equals(instrument.Name, UnknownCommitInstrument, StringComparison.Ordinal))
                target.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref count, int.CreateChecked(value)));
        listener.Start();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, RecoveredCache(), new AcceptingGateway(), (TimeProvider.System, new ReplicationMetrics(meter)));
        await SetAsync(committer, "k0", cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        var (_, factory) = await committer.EnsureStartedAsync(true, cancellationToken);
        var held = factory.PrepareSet(NewOperationId(), Cache, "k0", Entry(), 1UL);
        var error = new IOException("Injected local append failure.");

        var silent = await committer.MapPreAppendFailureAsync(error, held, 0UL, false);
        var countedAfterSilent = Volatile.Read(ref count);
        var reported = await committer.MapPreAppendFailureAsync(error, held, 0UL);

        _ = await Assert.That(silent is SquirixException { Code: SquirixErrorCode.CommitOutcomeUnknown }).IsTrue();
        _ = await Assert.That(reported is SquirixException { Code: SquirixErrorCode.CommitOutcomeUnknown }).IsTrue();
        _ = await Assert.That((countedAfterSilent, Volatile.Read(ref count))).IsEqualTo((0, 1));
    }

    /// <summary>
    /// A budget that ends while the batch is formed refuses the write being prepared, commits the writes prepared before it, and leaves the writes behind
    /// it queued for the next batch.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BudgetEndWhileFormingKeepsPrefix(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        var budgetTime = new FakeTimeProvider();
        await using var registry = await OpenRegistryAsync(DefaultOptions(), cancellationToken);
        await using var committer = CreateCommitter(registry, local, new AcceptingGateway(), budgetTime);
        var held = await committer.Gate.LockAsync(cancellationToken);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var leader = SetAsync(committer, "k1", cancellationToken);
        var expiring = PrepareWriteAsync(committer, "k2", (_, _, token) => WaitForCancellationAsync(entered, token), cancellationToken);
        var behind = SetAsync(committer, "k3", cancellationToken);
        held.Dispose();
        await entered.Task.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        budgetTime.Advance(SmallBudget);

        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(expiring.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
        await Task.WhenAll(leader, behind).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(Applied(local)).IsEqualTo("k1,k3");
        _ = await Assert.That(committer.CommitRoundTotals).IsEqualTo((2L, 2L));
    }

    private static async ValueTask<PreparedReplicaMutation> PrepareAfterAsync(ReplicaMutationFactory factory, string key, ulong index, TaskCompletionSource entered, TaskCompletionSource proceed)
    {
        _ = entered.TrySetResult();
        await proceed.Task.ConfigureAwait(false);
        return factory.PrepareSet(NewOperationId(), Cache, key, Entry(), index);
    }

    private static async ValueTask<PreparedReplicaMutation> WaitForCancellationAsync(TaskCompletionSource entered, CancellationToken cancellationToken)
    {
        _ = entered.TrySetResult();
        await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken).ConfigureAwait(false);
        throw new InvalidOperationException("The wait for the budget ended without a cancellation.");
    }

    private Task<ReplicaGroupRegistry> OpenRegistryAsync(FollowerLogOptions options, CancellationToken cancellationToken) => ReplicaCommitBatchKit.OpenRegistryAsync(Dir, options, cancellationToken);
}
