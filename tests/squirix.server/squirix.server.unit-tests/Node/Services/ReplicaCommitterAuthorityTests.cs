using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// An elected committer checks its authority under the commit gate right before the append: a write that waited while the election state
/// changed is refused definitely, and a local append the log refuses for a stale term is reported as stale-term. Nothing is appended in
/// either case. Node n1 leads group n2 in term 2.
/// </summary>
public sealed class ReplicaCommitterAuthorityTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Group = "n2";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A higher term seen after the write was routed refuses it with stale-term before the append.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeposedLeaderRefusesBeforeAppend(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-deposed");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var (committer, log) = await LeadAsync(registry, new ScriptedGateway(), TimeProvider.System, cancellationToken);
        await using var owned = committer;
        registry.StateFor(Group).ObserveHigherTerm(3UL);
        var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(last);
    }

    /// <summary>Authority the election state grants in another term than the tenure of the committer refuses retryably.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TenureTermMismatchRefuses(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-term");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var (committer, log) = await LeadAsync(registry, new ScriptedGateway(), TimeProvider.System, cancellationToken);
        await using var owned = committer;
        var state = registry.StateFor(Group);
        _ = state.BecomeLeader(4UL);
        _ = state.GrantAuthority(4UL);
        var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(last);
    }

    /// <summary>A log whose durable term moved past the led term refuses the local append; the write fails with stale-term, nothing appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LogStaleTermMapsToStaleTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-log-term");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var (committer, log) = await LeadAsync(registry, new ScriptedGateway(), TimeProvider.System, cancellationToken);
        await using var owned = committer;
        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That(refused.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-term");
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(last);
    }

    /// <summary>After a stale-term append, the restart of the dropped coordinator finds the higher log term and refuses with stale-term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartAfterStaleTermIsStaleTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-restart");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var (committer, log) = await LeadAsync(registry, new ScriptedGateway(), TimeProvider.System, cancellationToken);
        await using var owned = committer;
        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        _ = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That((committer.IsStarted, (await log.GetStatusAsync(cancellationToken)).LastLogIndex)).IsEqualTo((false, last));
    }

    /// <summary>A higher term seen while the write waits for the restart of the coordinator refuses it by the last check, before the append.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TermSeenDuringStartRefuses(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-during-start");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var gateway = new ProbeHoldingGateway();
        var (committer, log) = await LeadAsync(registry, gateway, TimeProvider.System, cancellationToken);
        await using var owned = committer;
        committer.DropStartedState();
        var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        gateway.HoldProbes = true;

        var write = committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        await gateway.ProbeHeld.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        registry.StateFor(Group).ObserveHigherTerm(3UL);
        gateway.ReleaseProbes();

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(write.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(last);
    }

    /// <summary>A retry of an operation whose entry is in the log but unresolved stays unknown on a deposed leader, never a stale marker.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnresolvedRetryOnDeposedIsUnknown(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-unresolved");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var gateway = new ProbeHoldingGateway();
        var clock = new FakeTimeProvider();
        var (committer, log) = await LeadAsync(registry, gateway, clock, cancellationToken);
        await using var owned = committer;
        var operationId = NewOperationId();
        gateway.HoldEntries = true;

        // The entry is appended locally and held at the followers until the commit budget expires: its outcome is unknown.
        var write = committer.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken);
        await gateway.EntriesHeld.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        clock.Advance(committer.CommitBudget * 2);
        var first = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));
        registry.StateFor(Group).ObserveHigherTerm(3UL);

        var retried = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(operationId, CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That((first.Code, retried.Code)).IsEqualTo((SquirixErrorCode.CommitOutcomeUnknown, SquirixErrorCode.CommitOutcomeUnknown));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL).Because("the unresolved entry stays in the log");
    }

    /// <summary>A leader whose election state has not granted authority yet refuses retryably.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PromotionPendingRefusesUnavailable(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-pending");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, cancellationToken);
        await using var committer = Create(registry, new ScriptedGateway(), TimeProvider.System);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        var state = registry.StateFor(Group);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(last);
    }

    /// <summary>Promotes n1 into term 2 of group n2, grants the authority, and commits one write to prove the leadership serves writes.</summary>
    /// <param name="registry">The registry of node n1.</param>
    /// <param name="gateway">Follower transport double.</param>
    /// <param name="budgetClock">The time source of the commit budget.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The authorized committer, owned by the caller, and the group log.</returns>
    private static async Task<(ReplicaGroupCommitter Committer, IFollowerLog Log)> LeadAsync(
        ReplicaGroupRegistry registry,
        IReplicaRpcGateway gateway,
        TimeProvider budgetClock,
        CancellationToken cancellationToken)
    {
        var log = await TermAsync(registry, cancellationToken);
        var committer = Create(registry, gateway, budgetClock);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        var state = registry.StateFor(Group);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        _ = await Assert.That(state.GrantAuthority(2UL)).IsTrue();
        await committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        return (committer, log);
    }

    private static ReplicaGroupCommitter Create(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, TimeProvider budgetClock)
    {
        var cache = new StubCache();
        return CreateElectedBudgetCommitter(registry, Group, gateway, (cache, new ReplicaGroupApplier(cache, NullLogger.Instance, Group, "n1")), budgetClock);
    }

    private static async Task<IFollowerLog> TermAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(Group, out var log))
            throw new InvalidOperationException($"The group log {Group} is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        return log;
    }

    /// <summary>Followers that hold the leader log, whose probes can be held until the test releases them and whose batches can be held until canceled.</summary>
    private sealed class ProbeHoldingGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _entriesHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ScriptedGateway _followers = new();
        private readonly TaskCompletionSource _never = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _probeHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task EntriesHeld => _entriesHeld.Task;

        internal bool HoldEntries { get; set; }

        internal bool HoldProbes { get; set; }

        internal Task ProbeHeld => _probeHeld.Task;

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Records.Count == 0 && HoldProbes)
            {
                _ = _probeHeld.TrySetResult();
                await new ValueTask(_released.Task).ConfigureAwait(false);
            }

            if (batch.Records.Count > 0 && HoldEntries)
            {
                _ = _entriesHeld.TrySetResult();
                await _never.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            return await _followers.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);
        }

        internal void ReleaseProbes() => _ = _released.TrySetResult();
    }
}
