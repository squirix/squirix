using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
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
/// Faults and races around a leadership by election: a start that fails after its leader-term entry, a retirement during a catch-up, and
/// a leader-term entry another leader replaced. Node n1 leads group n2 from slot 2; its followers are n2 and n3.
/// </summary>
public sealed class ElectedLeadershipFaultTests : ServerUnitTestBase
{
    private const int OlderTermTailEventId = 4010;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A start that fails after it appended the leader-term entry is retried without a second entry: the promotion keeps its index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedStartKeepsOneNoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-start-fault");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new HoldingGateway { FailProbes = true };
        await using var committer = Elected(registry, gateway);

        _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(committer.PromoteAsync(2UL, cancellationToken));
        var noopIndex = committer.Tenure?.NoopIndex ?? 0UL;
        gateway.FailProbes = false;
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(cancellationToken);
        _ = await committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter("n2", NullLogger.Instance, null), cancellationToken);
        var authorized = await committer.PromoteAsync(2UL, cancellationToken);

        _ = await Assert.That((noopIndex, committer.Tenure?.NoopIndex ?? 0UL, authorized)).IsEqualTo((1UL, 1UL, true));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>
    /// A leadership whose start throws is still published once: the group joins the led set and its readiness loop is queued, so the start
    /// is retried; a later promotion of the same leadership queues nothing more.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowingStartStillPublishesLeadership(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-start-publish");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new HoldingGateway { FailProbes = true };
        await using var committers = new ReplicaGroupCommitters(_ => Elected(registry, gateway), new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")), "n1", Owners(), TimeProvider.System);

        _ = await NodeAsyncAssert.ThrowsAsync<NotSupportedException>(committers.PromoteAsync("n2", 2UL, cancellationToken));
        var led = committers.Leads("n2");
        var queued = committers.Promotions!.TryRead(out var promotion);
        gateway.FailProbes = false;
        _ = await committers.PromoteAsync("n2", 2UL, cancellationToken);

        _ = await Assert.That((led, queued, promotion.Committer.GroupId, promotion.Tenure.IsCancellationRequested)).IsEqualTo((true, true, "n2", false));
        _ = await Assert.That(committers.Promotions.TryRead(out _)).IsFalse();
        _ = await Assert.That(registry.StateFor("n2").HasAuthority).IsFalse();
    }

    /// <summary>
    /// A retirement while the readiness loop of the leadership catches a follower up neither faults the loop nor waits on it: the retirement
    /// returns, and the loop ends normally with the leadership.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetireDuringCatchUpEndsLoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-retire-catch-up");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new HoldingGateway { HoldEntries = true };
        await using var committers = new ReplicaGroupCommitters(
            _ => Elected(registry, gateway),
            new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")),
            "n1",
            Owners(),
            TimeProvider.System);
        using var service = new ReplicaGroupReadinessService(committers, new EventRecordingLogger(), TimeProvider.System);

        await service.StartAsync(cancellationToken);
        bool retired;
        try
        {
            _ = await committers.PromoteAsync("n2", 2UL, cancellationToken);
            await gateway.EntriesHeld.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            retired = await committers.RetireAsync("n2", cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(retired).IsTrue();
        _ = await Assert.That(service.ExecuteTask?.IsCompletedSuccessfully).IsTrue().Because("A retirement ends the leadership loop normally.");
    }

    /// <summary>
    /// A verification whose probe was out while the leadership retired admits nothing: it reports blocked and starts no coordinator for the
    /// leadership that is over.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifyAfterRetireIsBlocked(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-verify-retire");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new HoldingGateway();
        await using var committer = Elected(registry, gateway);
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        gateway.HoldProbes = true;

        var verifying = committer.VerifyReplicasAsync(cancellationToken);
        await gateway.ProbesHeld.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var retired = await committer.RetireAsync(cancellationToken);
        gateway.ReleaseProbes();
        var verdict = await verifying.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That((retired, verdict)).IsEqualTo((true, ReplicaVerification.Blocked));
        _ = await Assert.That(committer.RunningPipeline).IsNull();
    }

    /// <summary>
    /// A write that holds the committer of a leadership that retired reaches the next leadership before its leader-term entry is committed:
    /// it is refused as retryable, and nothing is appended.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteIntoUnauthorizedTenureIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-write-next-tenure");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new HoldingGateway { MatchFollowers = true };
        await using var committer = Elected(registry, gateway);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        _ = await committer.RetireAsync(cancellationToken);
        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        gateway.DownFollowers = true;
        _ = await committer.PromoteAsync(3UL, cancellationToken);
        var appended = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), "cache", "b", Entry("b"), cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(appended);
    }

    /// <summary>A leader-term entry that a leader of a higher term replaced in the log never authorizes the leadership of the old term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplacedNoopNeverAuthorizes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-replaced-noop");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new HoldingGateway { DownFollowers = true };
        await using var committer = Elected(registry, gateway);
        _ = await committer.PromoteAsync(2UL, cancellationToken);

        // Leader n3 of term 3 replaces the uncommitted entry at index 1 with its own and commits it.
        var foreign = new ReplicaMutationFactory(new StubCache(), "n2", 3UL, TimeProvider.System, NullLogger.Instance).PrepareLeaderTerm(1UL);
        FollowerLogEntry[] entries = [new(1UL, 3UL, foreign.CanonicalPayload)];
        var replaced = await log.AppendAsync(new FollowerLogAppendRequest("n3", 3UL, 0UL, 0UL, 1UL, entries), cancellationToken);
        gateway.DownFollowers = false;

        _ = await Assert.That(replaced.Success).IsTrue();
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsFalse();
        _ = await Assert.That(await log.GetTermAtAsync(1UL, cancellationToken)).IsEqualTo(3UL);
    }

    /// <summary>
    /// A leadership whose leader-term entry append failed leaves a tail of an older term: verification reports it blocked with a warning
    /// once per blocked state, and warns again when the blocked tail grows.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedNoopLeavesOlderTermTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-failed-noop");
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, new FollowerLogOptions { FaultHooks = hooks }, cancellationToken);
        var log = await TermAsync(registry, "n2", 1UL, cancellationToken);
        var clock = new FakeTimeProvider();
        _ = await log.AppendAsync(new FollowerLogAppendRequest("n2", 1UL, 0UL, 0UL, 0UL, OlderEntry(1UL, clock)), cancellationToken);
        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var events = new EventRecordingLogger();
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        await using var committer = CreateElectedBudgetCommitter(registry, "n2", new HoldingGateway(), (cache, applier), clock, events);

        hooks.StallNextFrameWrite();
        var promotion = committer.PromoteAsync(2UL, cancellationToken);
        await hooks.Entered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        hooks.ReleaseWithFailure(new System.IO.IOException("Injected leader-term entry write failure."));
        await hooks.Exited.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var authorized = await promotion.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That((authorized, committer.Tenure?.Term, committer.Tenure?.NoopIndex)).IsEqualTo((false, 2UL, 0UL));
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
        _ = await Assert.That(events.Count(OlderTermTailEventId)).IsEqualTo(1);
        _ = await Assert.That(events.Find(OlderTermTailEventId)?.Level).IsEqualTo(LogLevel.Warning);

        _ = await log.AppendAsync(new FollowerLogAppendRequest("n1", 2UL, 1UL, 1UL, 0UL, OlderEntry(2UL, clock)), cancellationToken);
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);

        _ = await Assert.That(events.Count(OlderTermTailEventId)).IsEqualTo(2);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.LastLogIndex, status.LastLogTerm, status.CommitIndex)).IsEqualTo((2UL, 2UL, 1UL, 0UL));
    }

    private static FollowerLogEntry[] OlderEntry(ulong index, TimeProvider clock)
    {
        var record = new ReplicaMutationFactory(new StubCache(), "n2", 1UL, clock, NullLogger.Instance).PrepareLeaderTerm(index);
        return [new FollowerLogEntry(index, 1UL, record.CanonicalPayload)];
    }

    private static ReplicaGroupCommitter Elected(ReplicaGroupRegistry registry, HoldingGateway gateway)
    {
        var cache = new StubCache();
        return CreateElectedCommitter(registry, "n2", gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1"));
    }

    /// <summary>Makes a term durable in a group log, as the election driver does before it promotes.</summary>
    /// <param name="registry">The registry serving the group.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="term">The term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The group log.</returns>
    /// <exception cref="InvalidOperationException">The group log is not open.</exception>
    private static async Task<IFollowerLog> TermAsync(ReplicaGroupRegistry registry, string groupId, ulong term, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(groupId, out var log))
            throw new InvalidOperationException($"The group log {groupId} is not open.");

        _ = await log.ObserveTermAsync(term, cancellationToken);
        return log;
    }

    /// <summary>
    /// Followers behind the leader that hold nothing of the group, answered by <see cref="ScriptedGateway" />. Batches with entries can be
    /// held until their request is canceled; probes can be held until the test releases them, past their wall-clock probe timeout, or fail
    /// with a fault no probe expects; and every call can fail as from a down follower.
    /// </summary>
    private sealed class HoldingGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _entriesHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _probesHeld = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _probesReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ScriptedGateway _followers = new();

        internal HoldingGateway()
        {
            _followers.Set("n2", FollowerMode.Behind);
            _followers.Set("n3", FollowerMode.Behind);
        }

        internal bool DownFollowers { get; set; }

        internal Task EntriesHeld => _entriesHeld.Task;

        internal bool HoldProbes { get; set; }

        internal bool MatchFollowers { get; set; }

        internal Task ProbesHeld => _probesHeld.Task;

        internal bool FailProbes { get; set; }

        internal bool HoldEntries { get; set; }

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (DownFollowers)
                throw new System.IO.IOException("follower is down");

            var empty = batch.Records.Count == 0;
            if (empty && FailProbes)
                throw new NotSupportedException("Injected probe fault after the leader-term entry.");

            if (empty && HoldProbes)
            {
                _ = _probesHeld.TrySetResult();
                await new ValueTask(_probesReleased.Task).ConfigureAwait(false);
            }

            if (!empty && HoldEntries)
            {
                _ = _entriesHeld.TrySetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }

            if (!MatchFollowers)
                return await _followers.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);

            var last = empty ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            return new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last);
        }

        internal void ReleaseProbes() => _ = _probesReleased.TrySetResult();
    }
}
