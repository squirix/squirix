using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaPipelineTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The owner commit pipeline queues every locally appended entry on the follower senders as part of the local append.</summary>
public sealed class ReplicaGroupCommitPipelineTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The local append alone sends the entry to every follower, so an exception between the append and the fan-out of the coordinator
    /// cannot leave it unsent; the fan-out then takes the queued acknowledgements.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LocalAppendQueuesEveryFollower(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        try
        {
            var mutation = Mutation(1);

            await pipeline.AppendLocalAsync([mutation], cancellationToken);

            await routing.AppendedAsync("n2", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var acknowledgement = await pipeline.AppendFollowerAsync(2, mutation, cancellationToken).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(acknowledgement.LogIndex).IsEqualTo(1UL);
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>The fan-out of an entry this pipeline did not append last is refused instead of sending it out of order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FanOutOfOtherEntryIsRefused(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        try
        {
            await pipeline.AppendLocalAsync([Mutation(1)], cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pipeline.AppendFollowerAsync(1, Mutation(2), cancellationToken).AsTask());
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>
    /// A leader at slot 1 sends to slots 0 and 2 through its two senders: the fan-out of slot 2 takes the acknowledgement of the sender of
    /// n3, and a lagging slot 0 is reported as n1.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderAtSlotOneMapsSenders(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken, "n1", "n3");
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var eligibility = new ReplicaEligibility(3);
        var ready = new ReplicaProgress(1, 0, 0, 0, 0, new byte[] { 9, 8, 7 }, 1, 0);
        _ = eligibility.TryMarkReady(0, in ready, in ready);
        var logger = new EventRecordingLogger();
        var lagging = new ReplicaLaggingFollowers(GroupId, eligibility, new ReplicaRepairQueue(3), logger);
        var pipeline = await CreatePipelineAsync(registry, routing, ("n2", 1), lagging, cancellationToken);
        try
        {
            var mutation = Mutation(1);
            routing.ParkNext("n3");

            await pipeline.AppendLocalAsync([mutation], cancellationToken);
            await routing.Parked.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var toFirst = FanOutAsync(pipeline, 0, mutation, cancellationToken);
            var toThird = FanOutAsync(pipeline, 2, mutation, cancellationToken);
            var first = await toFirst.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var parked = toThird.IsCompleted;
            routing.Release();
            var third = await toThird.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            pipeline.RecordLaggingReplica(0, 1);

            _ = await Assert.That((first.LogIndex, third.LogIndex, parked)).IsEqualTo((1UL, 1UL, false));
            _ = await Assert.That(logger.FindMessage(4026)).Contains("follower n1 ", StringComparison.Ordinal);
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>
    /// A static leader sends the commit index to idle followers once it advances, so the last committed entries do not wait for a write
    /// that may never come; advancing to the same index again sends nothing more.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticAdvanceHeartbeatsIdleFollowers(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        try
        {
            await pipeline.AppendLocalAsync([Mutation(1)], cancellationToken);
            await AwaitAppendedAsync(routing, cancellationToken);

            await pipeline.AdvanceCommitIndexAsync(1, cancellationToken);
            var first = await routing.Heartbeats.Reader.ReadAsync(cancellationToken).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var second = await routing.Heartbeats.Reader.ReadAsync(cancellationToken).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await pipeline.AdvanceCommitIndexAsync(1, cancellationToken);

            _ = await Assert.That((first.PrevLogIndex, first.CommitIndex, second.PrevLogIndex, second.CommitIndex)).IsEqualTo((1UL, 1UL, 1UL, 1UL));
            _ = await Assert.That(first.Node).IsNotEqualTo(second.Node);
            _ = await Assert.That(routing.Heartbeats.Reader.Count).IsEqualTo(0);
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>An elected leader heartbeats on its own schedule, so advancing its commit index sends nothing extra.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ElectedAdvanceSendsNoHeartbeat(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken, new ReplicaReadIndexRound(1, 3, 0));
        try
        {
            await pipeline.AppendLocalAsync([Mutation(1)], cancellationToken);
            await AwaitAppendedAsync(routing, cancellationToken);

            await pipeline.AdvanceCommitIndexAsync(1, cancellationToken);

            _ = await Assert.That(routing.Heartbeats.Reader.Count).IsEqualTo(0);
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>A log that is disposed under an append says nothing about its health: the append is not counted, and nothing stays in flight.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposedLogIsNotCounted(CancellationToken cancellationToken)
    {
        var appends = new ReplicaLocalAppends();
        var pipeline = CreateRecordingPipeline(Task.FromException<FollowerLogAppendResult>(new ObjectDisposedException("log")), appends);

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(pipeline.AppendLocalAsync([Mutation(1)], cancellationToken).AsTask());

        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(0UL, 0UL, 1UL, false, false));
    }

    /// <summary>An append that ran out of its budget while it waited for the gate of the log is a failed append, and nothing stays in flight.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BudgetCancellationIsCounted(CancellationToken cancellationToken)
    {
        var appends = new ReplicaLocalAppends();
        var pipeline = CreateRecordingPipeline(Task.FromException<FollowerLogAppendResult>(new OperationCanceledException()), appends);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pipeline.AppendLocalAsync([Mutation(1)], cancellationToken).AsTask());

        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, 1UL, false, false));
    }

    /// <summary>An append that throws and leaves the log latched is a refusal that stays, not a plain failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrownAppendLeavingLatchedLogIsRefused(CancellationToken cancellationToken)
    {
        var appends = new ReplicaLocalAppends();
        var pipeline = CreateRecordingPipeline(Task.FromException<FollowerLogAppendResult>(new IOException("write failed")), appends, FollowerLogReadiness.Failed);

        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(pipeline.AppendLocalAsync([Mutation(1)], cancellationToken).AsTask());

        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, 1UL, false, true));
    }

    /// <summary>An append that succeeds is counted and clears the blocked mark a refusal left.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SuccessClearsRefusal(CancellationToken cancellationToken)
    {
        var appends = new ReplicaLocalAppends();
        appends.Begin();
        appends.End(ReplicaLocalAppends.Outcome.Refused);
        var pipeline = CreateRecordingPipeline(Task.FromResult(new FollowerLogAppendResult(true, string.Empty, 1UL, 1UL)), appends);

        await pipeline.AppendLocalAsync([Mutation(1)], cancellationToken);

        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(1UL, 1UL, 2UL, false, false));
    }

    /// <summary>A drained dispose answers not ready: the write was refused by shutdown before anything was scheduled, and the append counts as a refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposedLogNotReadyIsShutdownRefusal(CancellationToken cancellationToken)
    {
        var appends = new ReplicaLocalAppends();
        var pipeline = CreateRecordingPipeline(Task.FromResult(NotReady()), appends, FollowerLogReadiness.Ready, true);

        _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(pipeline.AppendLocalAsync([Mutation(1)], cancellationToken).AsTask());

        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, 1UL, false, true));
    }

    /// <summary>A not-ready answer of a log that is not disposed stays a plain refusal, and the append counts as a refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OpenLogNotReadyStaysPlainRefusal(CancellationToken cancellationToken)
    {
        var appends = new ReplicaLocalAppends();
        var pipeline = CreateRecordingPipeline(Task.FromResult(NotReady()), appends, FollowerLogReadiness.Ready, false);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pipeline.AppendLocalAsync([Mutation(1)], cancellationToken).AsTask());

        _ = await Assert.That(appends.Snapshot()).IsEqualTo(new ReplicaAppendProgress(0UL, 1UL, 1UL, false, true));
    }

    private static async Task AwaitAppendedAsync(FollowerLogRoutingGateway routing, CancellationToken cancellationToken)
    {
        await routing.AppendedAsync("n2", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    private static FollowerLogAppendResult NotReady() => new(false, FollowerLogRefusal.NotReady, 1UL, 0UL);

    private static Task<ReplicaDurableAcknowledgement> FanOutAsync(ReplicaGroupCommitPipeline pipeline, int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        pipeline.AppendFollowerAsync(replicaIndex, mutation, cancellationToken).AsTask();

    private static ReplicaGroupCommitPipeline CreateRecordingPipeline(Task<FollowerLogAppendResult> answer, ReplicaLocalAppends appends, FollowerLogReadiness readiness = FollowerLogReadiness.Ready, bool? disposed = null)
    {
        var log = new IFollowerLogCreateExpectations();
        _ = log.Setups.AppendAsync(Arg.Any<FollowerLogAppendRequest>(), Arg.Any<CancellationToken>()).ReturnValue(answer);
        _ = log.Setups.Readiness.Gets().ReturnValue(readiness);
        if (disposed is { } isDisposed)
            _ = log.Setups.IsDisposed.Gets().ReturnValue(isDisposed);

        var status = default(FollowerLogStatus);
        var lagging = new ReplicaLaggingFollowers(GroupId, new ReplicaEligibility(3), new ReplicaRepairQueue(3), NullLogger.Instance);
        return new ReplicaGroupCommitPipeline(new ReplicaGroupApplier(new StubCache(), NullLogger.Instance), log.Instance(), [], (GroupId, 0), lagging, in status, 1)
        {
            LocalAppends = appends,
        };
    }
}
