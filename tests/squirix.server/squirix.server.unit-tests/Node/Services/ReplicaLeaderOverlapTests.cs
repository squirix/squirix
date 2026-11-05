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

/// <summary>Replication may overlap a leader's local flush, but never substitutes for local durability.</summary>
public sealed class ReplicaLeaderOverlapTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(5);

    /// <summary>A durable follower majority cannot complete the commit while the leader's local flush is held.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task CommitWaitsForLocalFlush(CancellationToken cancellationToken) => VerifyCommitAsync(false, cancellationToken);

    /// <summary>A local flush failure after replication produces an unknown outcome, never success or definite refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task FailedLocalFlushIsUnknown(CancellationToken cancellationToken) => VerifyCommitAsync(true, cancellationToken);

    /// <summary>Followers acknowledge while the local flush is held; the local append completes only after that flush.</summary>
    /// <param name="count">The number of entries in the run.</param>
    /// <param name="cancelAfterScheduling">Whether the caller cancels after both followers durably receive the run.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1, false)]
    [Arguments(3, false)]
    [Arguments(3, true)]
    public async Task ReplicasRunBeforeLocalFlush(int count, bool cancelAfterScheduling, CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), new FollowerLogOptions { FaultHooks = hooks, TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        hooks.StallNextFrameWrite();
        var mutations = new PreparedReplicaMutation[count];
        for (var index = 0; index < count; index++)
            mutations[index] = Mutation(ulong.CreateChecked(index + 1));

        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var appending = pipeline.AppendLocalAsync(mutations, caller.Token).AsTask();
        try
        {
            await hooks.Entered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await routing.AppendedAsync("n2", ulong.CreateChecked(count)).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await routing.AppendedAsync("n3", ulong.CreateChecked(count)).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            if (cancelAfterScheduling)
                await caller.CancelAsync();

            _ = await Assert.That(appending.IsCompleted).IsFalse();
            hooks.Release();
            await appending.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            for (var index = 0; index < count; index++)
            {
                var first = await pipeline.AppendFollowerAsync(1, mutations[index], cancellationToken);
                var second = await pipeline.AppendFollowerAsync(2, mutations[index], cancellationToken);
                _ = await Assert.That((first.LogIndex, second.LogIndex)).IsEqualTo((ulong.CreateChecked(index + 1), ulong.CreateChecked(index + 1)));
            }

            _ = await Assert.That(pipeline.NextLogIndex).IsEqualTo(ulong.CreateChecked(count + 1));
        }
        finally
        {
            hooks.Release();
            await appending.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>A static leader cannot expose an entry that its local flush fails to retain.</summary>
    /// <param name="failFlush">Whether the held local flush fails.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StaticLeaderWaitsForLocalDurability(bool failFlush, CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), new FollowerLogOptions { FaultHooks = hooks, TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken, allowEarlyReplication: false);
        hooks.StallNextFrameWrite();
        var mutation = Mutation(1);
        var appending = pipeline.AppendLocalAsync([mutation], cancellationToken).AsTask();
        try
        {
            await hooks.Entered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(appending.IsCompleted).IsFalse();
            if (failFlush)
            {
                hooks.ReleaseWithFailure(new IOException("Injected static leader flush failure."));
                _ = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(appending, HangGuard, TimeProvider.System, cancellationToken);
                _ = await pipeline.CloseAsync();
                _ = await Assert.That(routing.Arrivals.IsEmpty).IsTrue();
                _ = await Assert.That(pipeline.NextLogIndex).IsEqualTo(1UL);
            }
            else
            {
                _ = await Assert.That(routing.Arrivals.IsEmpty).IsTrue();
                hooks.Release();
                await appending.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
                _ = await Assert.That((await pipeline.AppendFollowerAsync(1, mutation, cancellationToken)).LogIndex).IsEqualTo(1UL);
                _ = await Assert.That((await pipeline.AppendFollowerAsync(2, mutation, cancellationToken)).LogIndex).IsEqualTo(1UL);
            }
        }
        finally
        {
            hooks.Release();
            await appending.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>The static pipeline does not register a callback that can publish a locally unflushed entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticPipelineUsesDurableAppend(CancellationToken cancellationToken)
    {
        var log = new IFollowerLogCreateExpectations();
        _ = log.Setups.AppendAsync(Arg.Any<FollowerLogAppendRequest>(), Arg.Any<CancellationToken>())
               .ReturnValue(Task.FromResult(new FollowerLogAppendResult(true, string.Empty, 1UL, 1UL)));
        var status = default(FollowerLogStatus);
        var lagging = new ReplicaLaggingFollowers("n1", new ReplicaEligibility(3), new ReplicaRepairQueue(3), NullLogger.Instance);
        var pipeline = new ReplicaGroupCommitPipeline(new ReplicaGroupApplier(new StubCache(), NullLogger.Instance), log.Instance(), [], ("n1", 0), lagging, in status, 1);
        try
        {
            await pipeline.AppendLocalAsync([Mutation(1)], cancellationToken);
            _ = await Assert.That(pipeline.NextLogIndex).IsEqualTo(2UL);
            log.Verify();
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    private async Task VerifyCommitAsync(bool failFlush, CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), new FollowerLogOptions { FaultHooks = hooks, TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }, cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned log was not opened.");

        await using var coordinator = new ReplicaCommitCoordinator(new ReplicaCommitCoordinatorOptions(3, 0, 0, 4), pipeline, ReplicaFaultHooks.CreateNoOp(), log.Idempotency)
        {
            BudgetTimeProvider = TimeProvider.System,
            ObserveTimeProvider = TimeProvider.System,
        };
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1, TimeProvider.System, NullLogger.Instance);
        var mutation = factory.PrepareSet("op-1", "cache", "key", Entry("value"), 1);
        hooks.StallNextFrameWrite();
        var committing = coordinator.CommitAsync(mutation, TimeSpan.FromSeconds(30), cancellationToken).AsTask();
        try
        {
            await hooks.Entered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await routing.AppendedAsync("n2", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await routing.AppendedAsync("n3", 1).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            _ = await Assert.That(committing.IsCompleted).IsFalse();
            _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(0UL);
            _ = await Assert.That(coordinator.MatchIndexFor(0)).IsEqualTo(0UL);
            if (failFlush)
            {
                hooks.ReleaseWithFailure(new IOException("Injected local flush failure."));
                var failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(committing, HangGuard, TimeProvider.System, cancellationToken);
                _ = await Assert.That(failure.Message).StartsWith(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
                _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(0UL);
                _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            }
            else
            {
                hooks.Release();
                _ = await committing.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
                _ = await Assert.That(coordinator.CommitIndex).IsEqualTo(1UL);
                _ = await Assert.That(coordinator.MatchIndexFor(0)).IsEqualTo(1UL);
            }
        }
        finally
        {
            hooks.Release();
            Task completion = committing;
            await completion.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            _ = await pipeline.CloseAsync();
        }
    }
}
