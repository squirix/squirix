using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Durable majority of pipeline ordering and ownership tests.</summary>
[Immutable]
public sealed class DurableReplicationPipelineTests : ServerUnitTestBase
{
    private const string TailOperationA = "0000000000000000000000000000000a";
    private const string TailOperationB = "0000000000000000000000000000000b";

    /// <summary>The golden trace proves every durable and memory boundary is ordered.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppliesMemoryAfterMajorityCommit(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(1);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        var mutation = CreateMutation();
        try
        {
            var outcome = await coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), cancellationToken);
            await SequenceAssert.EqualAsync<byte>([7], outcome.ToArray());
            var retryOutcome = await coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), cancellationToken);
            await SequenceAssert.EqualAsync(outcome.ToArray(), retryOutcome.ToArray());
            _ = await Assert.That(pipeline.FollowerCalls).IsEqualTo(2);
            _ = await Assert.That(pipeline.MemoryApplyCount).IsEqualTo(1);
            IReadOnlyList<string> list =
            [
                "stage:Prepared",
                "local:1",
                "stage:LocalAppendDurable",
                "send:1",
                "send:2",
                "stage:FollowerFanOutStarted",
                "stage:MajorityReached",
                "commit:1",
                "stage:CommitIndexDurable",
                "apply:1",
                "stage:MemoryApplied",
                "stage:ResponseReady",
            ];
            await SequenceAssert.EqualAsync(list, pipeline.Trace, StringComparer.Ordinal);
            _ = await Assert.That(pipeline.LaggingReplicas).Contains(2);
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>Client cancellation after local durability does not abandon resolution or compensate memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationKeepsResolutionOwned(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(0);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        var mutation = CreateMutation();
        using var cancellation = new CancellationTokenSource();
        try
        {
            var operation = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(5), cancellation.Token);
            _ = await pipeline.LocalAppended.Task.WaitAsync(cancellationToken);
            await cancellation.CancelAsync();
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(pipeline.MemoryApplyCount).IsEqualTo(0);

            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
            _ = await Assert.That(pipeline.MemoryApplyCount).IsEqualTo(1);
        }
        finally
        {
            pipeline.ReleaseFollowers();
        }
    }

    /// <summary>A catching-up follower contributes no quorum copy until a repair session marks it ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CatchingUpFollowerCountsOnlyWhenReady(CancellationToken cancellationToken)
    {
        var eligibility = new ReplicaEligibility(3);
        var ready = Progress(1UL, 0UL, 0UL, 0UL, 1UL);
        _ = await Assert.That(eligibility.TryMarkReady(0, in ready, in ready)).IsTrue();

        var stalledPipeline = new RecordingPipeline(1);
        var stalledHooks = new RecordingHooks(stalledPipeline.Trace);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 4);
        var stalled = new ReplicaCommitCoordinator(options, stalledPipeline, stalledHooks, new GroupIdempotencyState(10, TimeSpan.MaxValue), eligibility);
        try
        {
            var stalledCommit = stalled.CommitAsync(CreateMutation(), TimeSpan.FromMilliseconds(200), cancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(stalledCommit);
            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        }
        finally
        {
            stalledPipeline.ReleaseFollowers();
            await stalled.DisposeAsync();
        }

        _ = await Assert.That(eligibility.TryMarkReady(1, in ready, in ready)).IsTrue();
        var pipeline = new RecordingPipeline(1);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(options, pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue), eligibility);
        try
        {
            var outcome = await coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(5), cancellationToken);
            await SequenceAssert.EqualAsync<byte>([7], outcome.ToArray());
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>Disposal observes stalled followers within the bound instead of hanging.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeDrainExpiresOnStalledFollowers(CancellationToken cancellationToken)
    {
        var pipeline = new ReplicaCommitTestKit.Pipeline(false, true);

        // A short drain bound instead of the 5 s default: the commit budget below stays well above it, so the bound still expires first.
        var coordinator = ReplicaCommitTestKit.CreateCoordinator(pipeline, shutdownBudget: TimeSpan.FromMilliseconds(200));
        try
        {
            // No follower ever answers: the commit parks in the majority wait while disposal drains it.
            var commit = coordinator.CommitAsync(ReplicaCommitTestKit.CreateMutation(), TimeSpan.FromSeconds(2), cancellationToken);

            // The drain bound expires in real time; disposal completes instead of hanging on the parked commit.
            // The test-side bound only guards against a drain regression; it sits far above the drain bound.
            await coordinator.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

            // Budget expiry faults the parked resolution after abandonment, running the attached observer.
            // Disposal left the gates with the running commit, so it unwinds with its own budget expiry, not ObjectDisposedException.
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(commit);
            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(error.InnerException).IsAssignableTo<OperationCanceledException>();
        }
        finally
        {
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>Disposal closes admission before draining an in-flight operation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeStopsAndDrains(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(1, true);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        Task? disposal = null;
        try
        {
            var currentOperation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(5), cancellationToken);
            var current = currentOperation.AsTask();
            _ = await pipeline.LocalAppended.Task.WaitAsync(cancellationToken);
            disposal = coordinator.DisposeAsync().AsTask();
            _ = await Assert.That(disposal.IsCompleted).IsFalse();

            var rejected = coordinator.CommitAsync(CreateMutation(2, "fedcba9876543210fedcba9876543210"), TimeSpan.FromSeconds(5), cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, ReadOnlyMemory<byte>>(rejected);

            pipeline.ReleaseLocalAppend();
            _ = await current;
            pipeline.ReleaseFollowers();
            await disposal;
        }
        finally
        {
            pipeline.ReleaseLocalAppend();
            pipeline.ReleaseFollowers();
            if (disposal == null)
                await coordinator.DisposeAsync();
        }
    }

    /// <summary>Follower work started before an exceptional exit remains owned until disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExceptionalFanOutStillOwnsFollowerTasks(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(0);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 4);
        var coordinator = new ReplicaCommitCoordinator(options, pipeline, ThrowOnFanOutHooks.Instance, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        Task? disposal = null;
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(5), cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);

            disposal = coordinator.DisposeAsync().AsTask();
            _ = await Assert.That(disposal.IsCompleted).IsFalse();
            pipeline.ReleaseFollowers();
            await disposal;
        }
        finally
        {
            pipeline.ReleaseFollowers();
            if (disposal == null)
                await coordinator.DisposeAsync();
        }
    }

    /// <summary>The next commit retries a memory-apply failure after the commit index advances.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedMemoryApplyIsRetriedByLaterCommit(CancellationToken cancellationToken)
    {
        var pipeline = new FlakyMemoryPipeline();
        var coordinator = CreateCoordinator(3, pipeline);
        try
        {
            var first = coordinator.CommitAsync(CreateMutation(1, "00000000000000000000000000000001"), TimeSpan.FromSeconds(2), cancellationToken);
            var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);
            _ = await Assert.That(firstError.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

            var outcome = await coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002"), TimeSpan.FromSeconds(2), cancellationToken);
            await SequenceAssert.EqualAsync<byte>([7], outcome.ToArray());
        }
        finally
        {
            await coordinator.DisposeAsync();
        }

        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.AppliedIndexes);
    }

    /// <summary>A late acknowledgement advances its replica before that replica's next acknowledgement is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateAcknowledgementSupportsNextCommit(CancellationToken cancellationToken)
    {
        var pipeline = new LateFollowerPipeline();
        var coordinator = CreateCoordinator(5, pipeline);
        try
        {
            _ = await coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(2), cancellationToken);
            var second = coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002"), TimeSpan.FromSeconds(2), cancellationToken);
            await pipeline.SecondReplicaThreeStarted.WaitAsync(cancellationToken);

            pipeline.ReleaseFirstReplicaThree();
            await pipeline.FirstReplicaThreeAcknowledged.WaitAsync(cancellationToken);

            // FirstReplicaThreeAcknowledged fires when the pipeline produces the index 1
            // acknowledgement, before the coordinator records it through TryRecord on the
            // background observe path. Wait until replica 3's match index actually advances
            // to 1, so the buffered index 2 acknowledgement cannot overtake it.
            var match = (Coordinator: coordinator, ReplicaIndex: 3, MatchIndex: 1UL);
            await match.WaitUntilAsync(static s => s.Coordinator.MatchIndexFor(s.ReplicaIndex) >= s.MatchIndex, cancellationToken);
            pipeline.ReleaseSecondReplicaThree();

            _ = await second;
            _ = await Assert.That(pipeline.MemoryApplyCount).IsEqualTo(2);
        }
        finally
        {
            pipeline.ReleaseFirstReplicaThree();
            pipeline.ReleaseSecondReplicaThree();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>A follower response completing after the deadline is still recorded, not marked lagging.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LateFollowerResponseIsRecorded(CancellationToken cancellationToken)
    {
        var pipeline = new DeferredFollowersPipeline();
        var coordinator = CreateCoordinator(3, pipeline);
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromMilliseconds(100), cancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

            // Wait for the observable deadline signal so late responses truly arrive after the deadline.
            await pipeline.DeadlineElapsed.WaitAsync(cancellationToken);
            pipeline.ReleaseFollowers(CreateMutation());
        }
        finally
        {
            await coordinator.DisposeAsync();
        }

        _ = await Assert.That(pipeline.FollowerCalls).IsEqualTo(2);
        await SequenceAssert.EqualAsync([], pipeline.LaggingReplicas);
    }

    /// <summary>A later commit applies skipped earlier entries in index order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LaterCommitAppliesSkippedEntries(CancellationToken cancellationToken)
    {
        var pipeline = new GatedFollowersPipeline();
        var coordinator = CreateCoordinator(3, pipeline, ThrowOnFirstMajorityHooks.Instance);
        try
        {
            // Release up front so every acknowledgement is recorded in the foreground drain loop.
            // The injected post-majority failure leaves index 1 retained but unapplied without
            // relying on the racy 100ms-budget-versus-background-observe ordering.
            pipeline.ReleaseFollowers();
            var first = coordinator.CommitAsync(CreateMutation(1, "00000000000000000000000000000001"), TimeSpan.FromSeconds(2), cancellationToken);
            var firstError = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(first);
            _ = await Assert.That(firstError.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);

            var outcome = await coordinator.CommitAsync(CreateMutation(2, "00000000000000000000000000000002"), TimeSpan.FromSeconds(2), cancellationToken);
            await SequenceAssert.EqualAsync<byte>([7], outcome.ToArray());
        }
        finally
        {
            await coordinator.DisposeAsync();
        }

        await SequenceAssert.EqualAsync([1UL, 2UL], pipeline.AppliedIndexes);
    }

    /// <summary>A lagging follower past the observe bound does not block disposal after majority commit.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ObserveBoundExpiresOnLaggingFollower(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(1);
        var hooks = new RecordingHooks(pipeline.Trace);
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 4);

        // The observe bound runs on an injected clock, so the test moves it past the 5 s bound instead of waiting in real time.
        var observeClock = new FakeTimeProvider();
        var coordinator = new ReplicaCommitCoordinator(options, pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue)) { ObserveTimeProvider = observeClock };
        try
        {
            // Leader plus follower 1 reach majority; follower 2 lags past the observe bound.
            var outcome = await coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(5), cancellationToken);
            await SequenceAssert.EqualAsync<byte>([7], outcome.ToArray());

            // Start disposal first so a stuck drain fails fast on the test-side bound instead of hanging.
            var disposal = coordinator.DisposeAsync().AsTask();

            // The observer registers its bound timer in the background, so keep advancing the clock briefly instead of assuming it exists yet.
            for (var i = 0; i < 20; i++)
            {
                observeClock.Advance(TimeSpan.FromSeconds(6));
                await Task.Delay(TimeSpan.FromMilliseconds(10), TimeProvider.System, cancellationToken);
            }

            // The bound already attached fault observers; faulting the laggard runs them for cleanup.
            pipeline.FailFollowers(new TimeoutException("Lagging follower fault."));
            await disposal.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// An uncommitted tail recovered at start stays retained while only the leader holds it, commits and applies in log order once a
    /// verified follower is admitted at its last index, and new writes continue after it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredTailCommitsOnAdmission(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(1);
        var tail = new ReplicaRecoveredTail([TailEntry(2, 1, TailOperationA), TailEntry(3, 1, TailOperationB)], 1, TailRebuilder.Instance);
        var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(3, 3, 1, 4),
            pipeline,
            new RecordingHooks(pipeline.Trace),
            new GroupIdempotencyState(10, TimeSpan.MaxValue),
            null,
            tail);
        try
        {
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsFalse();
            _ = await Assert.That(pipeline.Trace).IsEmpty();

            coordinator.AdmitReplica(1, 3);
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsTrue();
            _ = await coordinator.CommitAsync(CreateMutation(4), TimeSpan.FromSeconds(5), cancellationToken);

            IReadOnlyList<string> list =
            [
                "commit:3",
                "apply:2",
                "apply:3",
                "stage:Prepared",
                "local:4",
                "stage:LocalAppendDurable",
                "send:1",
                "send:2",
                "stage:FollowerFanOutStarted",
                "stage:MajorityReached",
                "commit:4",
                "stage:CommitIndexDurable",
                "apply:4",
                "stage:MemoryApplied",
                "stage:ResponseReady",
            ];
            await SequenceAssert.EqualAsync(list, pipeline.Trace, StringComparer.Ordinal);
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// A recovered entry keeps its idempotency pin: a same-identity retry before the commit reports an unknown outcome without
    /// appending anything, and after the commit it replays the outcome read right before the entry's apply.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredTailKeepsPinsUntilCommit(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(1);
        var idempotency = new GroupIdempotencyState(10, TimeSpan.MaxValue);
        var tail = new ReplicaRecoveredTail([TailEntry(2, 1, TailOperationA)], 1, TailRebuilder.Instance);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 2, 1, 4), pipeline, new RecordingHooks(pipeline.Trace), idempotency, null, tail);
        var retry = CreateMutation(3, TailOperationA);
        try
        {
            var unknown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(coordinator.CommitAsync(retry, TimeSpan.FromSeconds(5), cancellationToken));
            _ = await Assert.That(unknown.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(idempotency.Lookup(retry.OperationScope, retry.OperationId, retry.OperationFingerprint.Span, out _)).IsEqualTo(GroupIdempotencyLookup.Unresolved);

            coordinator.AdmitReplica(1, 2);
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsTrue();
            var replayed = await coordinator.CommitAsync(retry, TimeSpan.FromSeconds(5), cancellationToken);

            await SequenceAssert.EqualAsync<byte>([102], replayed.ToArray());
            await SequenceAssert.EqualAsync(["commit:2", "apply:2"], pipeline.Trace, StringComparer.Ordinal);
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>
    /// A recovered tail of an older term is not committed by counting replicas, even on a majority; a current-term entry above it
    /// commits it transitively, in log order.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredOlderTermTailWaits(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(1);
        var tail = new ReplicaRecoveredTail([TailEntry(2, 1, TailOperationA)], 2, TailRebuilder.Instance);
        var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(3, 2, 1, 4),
            pipeline,
            new RecordingHooks(pipeline.Trace),
            new GroupIdempotencyState(10, TimeSpan.MaxValue),
            null,
            tail);
        try
        {
            coordinator.AdmitReplica(1, 2);
            _ = await Assert.That(await coordinator.ApplyCommittedAsync()).IsFalse();
            _ = await Assert.That(pipeline.Trace).IsEmpty();

            var current = new PreparedReplicaMutation(
                new ReplicaOperationIdentity("group-a", "client", TailOperationB, new byte[] { 1, 2, 3 }),
                2,
                3,
                new ReplicaMutationPayload(new byte[] { 4, 5, 6 }, new byte[] { 7 }, 42));
            _ = await coordinator.CommitAsync(current, TimeSpan.FromSeconds(5), cancellationToken);

            _ = await Assert.That(pipeline.Trace).Contains("commit:3");
            await SequenceAssert.EqualAsync([2UL, 3UL], pipeline.AppliedIndexes);
        }
        finally
        {
            pipeline.ReleaseFollowers();
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>One shared task instance cannot count as acknowledgements from multiple replicas.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SharedFollowerTaskCountsOnce(CancellationToken cancellationToken)
    {
        var pipeline = new SharedFollowerTaskPipeline();
        var coordinator = CreateCoordinator(5, pipeline);
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromSeconds(2), cancellationToken);

            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ReadOnlyMemory<byte>>(operation);
            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
            _ = await Assert.That(pipeline.MemoryApplyCount).IsEqualTo(0);
        }
        finally
        {
            await coordinator.DisposeAsync();
        }
    }

    /// <summary>The timeout budget includes admission and local durable append work.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TimeoutIncludesLocalAppend(CancellationToken cancellationToken)
    {
        var pipeline = new RecordingPipeline(1, true);
        var hooks = new RecordingHooks(pipeline.Trace);
        var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, hooks, new GroupIdempotencyState(10, TimeSpan.MaxValue));
        try
        {
            var operation = coordinator.CommitAsync(CreateMutation(), TimeSpan.FromMilliseconds(100), cancellationToken);
            _ = await pipeline.LocalAppended.Task.WaitAsync(cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException, ReadOnlyMemory<byte>>(operation);
            _ = await Assert.That(pipeline.MemoryApplyCount).IsEqualTo(0);
        }
        finally
        {
            pipeline.ReleaseLocalAppend();
            await coordinator.DisposeAsync();
        }
    }

    private static ReplicaCommitCoordinator CreateCoordinator(int replicaCount, IReplicaCommitPipeline pipeline, IReplicaCommitFaultHooks? hooks = null)
    {
        if (hooks == null)
        {
            var noOpExpectations = new IReplicaCommitFaultHooksCreateExpectations();
            _ = noOpExpectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                                .ReturnValue(ValueTask.CompletedTask);
            hooks = noOpExpectations.Instance();
        }

        var options = new ReplicaCommitCoordinatorOptions(replicaCount, 0, 0, 8);
        return new ReplicaCommitCoordinator(options, pipeline, hooks, new GroupIdempotencyState(16, TimeSpan.MaxValue));
    }

    private static PreparedReplicaMutation CreateMutation(ulong logIndex = 1, string operationId = "0123456789abcdef0123456789abcdef") => new(
        new ReplicaOperationIdentity("group-a", "client", operationId, new byte[] { 1, 2, 3 }),
        1,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 4, 5, 6 }, new byte[] { 7 }, 42));

    private static FollowerLogEntry TailEntry(ulong logIndex, ulong term, string operationId)
    {
        // The canonical record carries the identity CreateMutation uses, so a same-identity retry matches the recovered pin.
        var record = new ReplicaLogRecord(logIndex, term, operationId, "client", new byte[] { 1, 2, 3 }, "UserMutation", "cache", Encoding.UTF8.GetBytes("k"), "Set", new byte[] { 4 }, ReadOnlyMemory<byte>.Empty, 0, 0, 0, 0);
        return new FollowerLogEntry(logIndex, term, ReplicaLogCodec.Encode(in record));
    }

    private static ReplicaProgress Progress(ulong nextIndex, ulong matchIndex, ulong commitIndex, ulong appliedIndex, ulong lastTerm) => new(
        nextIndex,
        matchIndex,
        commitIndex,
        appliedIndex,
        lastTerm,
        new byte[] { 8 },
        1UL,
        7U);

    [Mutable]
    private sealed class DeferredFollowersPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource<bool> _deadlineElapsed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _second = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task DeadlineElapsed => _deadlineElapsed.Task;

        internal int FollowerCalls { get; private set; }

        internal List<int> LaggingReplicas { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = cancellationToken.Register(() => _ = _deadlineElapsed.TrySetResult(true));
            FollowerCalls++;
            return new ValueTask<ReplicaDurableAcknowledgement>(replicaIndex == 1 ? _first.Task : _second.Task);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ApplyMemoryAsync(mutation, cancellationToken);

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex) => LaggingReplicas.Add(replicaIndex);

        internal void ReleaseFollowers(PreparedReplicaMutation mutation)
        {
            var acknowledgement = new ReplicaDurableAcknowledgement(
                mutation.GroupId,
                mutation.Term,
                mutation.LogIndex,
                mutation.OperationFingerprint,
                mutation.PayloadChecksum,
                true,
                true);
            _ = _first.TrySetResult(acknowledgement);
            _ = _second.TrySetResult(acknowledgement);
        }
    }

    [Mutable]
    private sealed class FlakyMemoryPipeline : IReplicaCommitPipeline
    {
        private bool _failedOnce;

        internal List<ulong> AppliedIndexes { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            ValueTask.FromResult(
                new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true));

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (!_failedOnce)
            {
                _failedOnce = true;
                return ValueTask.FromException(new InvalidOperationException("Injected memory-apply failure after commit index advanced."));
            }

            AppliedIndexes.Add(mutation.LogIndex);
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }

    [Mutable]
    private sealed class GatedFollowersPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource<bool> _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal List<ulong> AppliedIndexes { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            new(WaitAndAcknowledgeAsync(mutation, cancellationToken));

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            AppliedIndexes.Add(mutation.LogIndex);
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void ReleaseFollowers() => _ = _released.TrySetResult(true);

        private async Task<ReplicaDurableAcknowledgement> WaitAndAcknowledgeAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
        }
    }

    [Mutable]
    private sealed class LateFollowerPipeline : IReplicaCommitPipeline
    {
        private readonly TaskCompletionSource<bool> _firstReplicaThree = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _firstReplicaThreeAcknowledged = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _secondReplicaThree = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<bool> _secondReplicaThreeStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task FirstReplicaThreeAcknowledged => _firstReplicaThreeAcknowledged.Task;

        internal int MemoryApplyCount { get; private set; }

        internal Task SecondReplicaThreeStarted => _secondReplicaThreeStarted.Task;

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (replicaIndex == 3)
                return new ValueTask<ReplicaDurableAcknowledgement>(AcknowledgeReplicaThreeAsync(mutation, cancellationToken));

            var ready = mutation.LogIndex == 1 || replicaIndex == 1;
            return ValueTask.FromResult(BuildAcknowledgement(mutation, ready));
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            MemoryApplyCount++;
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        internal void ReleaseFirstReplicaThree() => _ = _firstReplicaThree.TrySetResult(true);

        internal void ReleaseSecondReplicaThree() => _ = _secondReplicaThree.TrySetResult(true);

        private static ReplicaDurableAcknowledgement BuildAcknowledgement(PreparedReplicaMutation mutation) => BuildAcknowledgement(mutation, true);

        private static ReplicaDurableAcknowledgement BuildAcknowledgement(PreparedReplicaMutation mutation, bool ready) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            ready);

        private async Task<ReplicaDurableAcknowledgement> AcknowledgeReplicaThreeAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (mutation.LogIndex == 1)
            {
                _ = await _firstReplicaThree.Task.WaitAsync(cancellationToken);
                _ = _firstReplicaThreeAcknowledged.TrySetResult(true);
            }
            else
            {
                _ = _secondReplicaThreeStarted.TrySetResult(true);
                _ = await _firstReplicaThree.Task.WaitAsync(cancellationToken);
                _ = await _secondReplicaThree.Task.WaitAsync(cancellationToken);
            }

            return BuildAcknowledgement(mutation);
        }
    }

    [Mutable]
    private sealed class RecordingHooks : IReplicaCommitFaultHooks
    {
        private readonly List<string> _trace;

        internal RecordingHooks(List<string> trace)
        {
            _trace = trace;
        }

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _trace.Add($"stage:{stage}");
            return ValueTask.CompletedTask;
        }
    }

    [Mutable]
    private sealed class RecordingPipeline : IReplicaCommitPipeline
    {
        private readonly bool _blockLocalAppend;

        private readonly TaskCompletionSource<ReplicaDurableAcknowledgement> _followers = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private readonly int _immediateFollowerIndex;

        private readonly TaskCompletionSource<bool> _localAppendRelease = new(TaskCreationOptions.RunContinuationsAsynchronously);

        private PreparedReplicaMutation? _recordedMutation;

        internal RecordingPipeline(int immediateFollowerIndex, bool blockLocalAppend = false)
        {
            _immediateFollowerIndex = immediateFollowerIndex;
            _blockLocalAppend = blockLocalAppend;
        }

        internal List<ulong> AppliedIndexes { get; } = [];

        internal int FollowerCalls { get; private set; }

        internal List<int> LaggingReplicas { get; } = [];

        internal TaskCompletionSource<bool> LocalAppended { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int MemoryApplyCount { get; private set; }

        internal List<string> Trace { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            Trace.Add($"commit:{commitIndex}");
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            FollowerCalls++;
            Trace.Add($"send:{replicaIndex}");
            _recordedMutation ??= mutation;
            return replicaIndex == _immediateFollowerIndex ? ValueTask.FromResult(CreateReadyAcknowledgement(mutation))
                : new ValueTask<ReplicaDurableAcknowledgement>(_followers.Task);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            Trace.Add($"local:{mutation.LogIndex}");
            _ = LocalAppended.TrySetResult(true);
            return _blockLocalAppend ? new ValueTask(_localAppendRelease.Task.WaitAsync(cancellationToken)) : ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            MemoryApplyCount++;
            AppliedIndexes.Add(mutation.LogIndex);
            Trace.Add($"apply:{mutation.LogIndex}");
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex) => LaggingReplicas.Add(replicaIndex);

        internal void FailFollowers(Exception exception)
        {
            ArgumentNullException.ThrowIfNull(exception);
            _ = _followers.TrySetException(exception);
        }

        internal void ReleaseFollowers()
        {
            if (_recordedMutation is not { } recorded)
                return;

            _ = _followers.TrySetResult(CreateReadyAcknowledgement(recorded));
        }

        internal void ReleaseLocalAppend() => _ = _localAppendRelease.TrySetResult(true);

        private static ReplicaDurableAcknowledgement CreateReadyAcknowledgement(PreparedReplicaMutation mutation) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);
    }

    [Mutable]
    private sealed class SharedFollowerTaskPipeline : IReplicaCommitPipeline
    {
        private Task<ReplicaDurableAcknowledgement>? _shared;

        internal int MemoryApplyCount { get; private set; }

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _shared ??= Task.FromResult(BuildAcknowledgement(mutation));
            return new ValueTask<ReplicaDurableAcknowledgement>(_shared);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            MemoryApplyCount++;
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }

        private static ReplicaDurableAcknowledgement BuildAcknowledgement(PreparedReplicaMutation mutation) => new(
            mutation.GroupId,
            mutation.Term,
            mutation.LogIndex,
            mutation.OperationFingerprint,
            mutation.PayloadChecksum,
            true,
            true);
    }

    /// <summary>Rebuilds recovered entries from their canonical record; the outcome read before the apply is the log index plus 100.</summary>
    [Immutable]
    private sealed class TailRebuilder : IReplicaTailRebuilder
    {
        internal static TailRebuilder Instance { get; } = new();

        public ValueTask<ReadOnlyMemory<byte>> ReadOutcomeAsync(PreparedReplicaMutation entry, CancellationToken cancellationToken) =>
            ValueTask.FromResult<ReadOnlyMemory<byte>>(new[] { Convert.ToByte(entry.LogIndex + 100) });

        public PreparedReplicaMutation Rebuild(FollowerLogEntry entry)
        {
            var record = ReplicaLogCodec.Decode(entry.Payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("Test tail entry is undecodable."));
            var identity = new ReplicaOperationIdentity("group-a", record.OperationScope, record.OperationId, record.OperationFingerprint);
            return new PreparedReplicaMutation(identity, entry.Term, entry.LogIndex, new ReplicaMutationPayload(entry.Payload, ReadOnlyMemory<byte>.Empty, 42));
        }
    }

    private sealed class ThrowOnFanOutHooks : IReplicaCommitFaultHooks
    {
        internal static ThrowOnFanOutHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            var exception = new InvalidOperationException("Injected fan-out failure.");
            return stage == ReplicaCommitStage.FollowerFanOutStarted ? ValueTask.FromException(exception) : ValueTask.CompletedTask;
        }
    }

    private sealed class ThrowOnFirstMajorityHooks : IReplicaCommitFaultHooks
    {
        internal static ThrowOnFirstMajorityHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            var exception = new InvalidOperationException("Injected post-majority failure.");
            return stage == ReplicaCommitStage.MajorityReached && mutation.LogIndex == 1 ? ValueTask.FromException(exception) : ValueTask.CompletedTask;
        }
    }
}
