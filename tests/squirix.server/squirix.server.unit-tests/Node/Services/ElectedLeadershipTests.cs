using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A committer leads a group only in a term the election hands it: it appends a leader-term entry of its own, is authorized only once a
/// coordinator of the promotion committed that entry, holds the applier lease for its tenure, and gives the group back on retirement.
/// Node n1 leads group n2 from slot 2; its followers are n2 and n3.
/// </summary>
public sealed class ElectedLeadershipTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>The event id of a group whose every replica slot is verified.</summary>
    private const int VerificationCompleteEventId = 4002;

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Followers that lack the leader-term entry start unverified; the entry is still committed, without the write majority check, once the
    /// catch-up hands it to them, and only then is the promotion authorized.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoopCommitsWhileFollowersStartUnverified(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-noop");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind);
        gateway.Set("n3", FollowerMode.Behind);
        await using var committer = Elected(registry, "n2", gateway);

        var first = await committer.PromoteAsync(2UL, cancellationToken);
        var verification = await committer.VerifyReplicasAsync(cancellationToken);
        var admitted = await committer.CatchUpFollowersAsync(new ReplicaCatchUpReporter("n2", NullLogger.Instance, null), cancellationToken);
        var authorized = await committer.PromoteAsync(2UL, cancellationToken);

        _ = await Assert.That((first, verification, admitted, authorized)).IsEqualTo((false, ReplicaVerification.Pending, true, true));
        var noops = await NoopsAsync(log, cancellationToken);
        _ = await Assert.That(noops.Count).IsEqualTo(1);
        _ = await Assert.That((noops[0].LogIndex, noops[0].Term)).IsEqualTo((1UL, 2UL));
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(1UL);
    }

    /// <summary>
    /// An owner restarted at the provisional term one appends a new leader-term entry: the committed entry of its earlier run does not
    /// authorize it, and without a majority it stays unauthorized.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartedTermOneOwnerNeedsFreshMajority(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-restart");
        await using (var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken))
        {
            _ = await TermAsync(registry, "n1", 1UL, cancellationToken);
            await using var committer = Elected(registry, "n1", new ScriptedGateway());
            _ = await Assert.That(await committer.PromoteAsync(1UL, cancellationToken)).IsTrue();
        }

        await using var reopened = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(reopened, "n1", 1UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        await using var restarted = Elected(reopened, "n1", gateway);

        var promoted = await restarted.PromoteAsync(1UL, cancellationToken);
        var verification = await restarted.VerifyReplicasAsync(cancellationToken);
        var retried = await restarted.PromoteAsync(1UL, cancellationToken);

        _ = await Assert.That((promoted, verification, retried)).IsEqualTo((false, ReplicaVerification.Pending, false));
        var noops = await NoopsAsync(log, cancellationToken);
        _ = await Assert.That(noops.Count).IsEqualTo(2);
        _ = await Assert.That(noops[0].OperationId).IsNotEqualTo(noops[1].OperationId);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(1UL);
    }

    /// <summary>A promotion waits for a running apply pass to give the lease back, and the apply loop is kept out until the group retires.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaseKeepsOneDriverPerGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-lease");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, "n2", "n1");
        await using var committer = Elected(registry, "n2", new ScriptedGateway(), applier);
        _ = applier.DriverLease.TryEnterPass();

        var promoting = committer.PromoteAsync(2UL, cancellationToken);
        var waited = !promoting.IsCompleted;
        applier.DriverLease.ExitPass();
        var authorized = await promoting.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var heldWhileLeading = !applier.DriverLease.TryEnterPass();
        var tenure = committer.Tenure?.Token ?? CancellationToken.None;
        var retired = await committer.RetireAsync(cancellationToken);
        var freed = applier.DriverLease.TryEnterPass();
        applier.DriverLease.ExitPass();

        _ = await Assert.That((waited, authorized, heldWhileLeading, retired, freed)).IsEqualTo((true, true, true, true, true));
        _ = await Assert.That(tenure.IsCancellationRequested).IsTrue();
        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
    }

    /// <summary>
    /// A leader that steps down while its leader-term entry is still uncommitted retires at once: the entry stays in the log for the next
    /// leader to commit or truncate.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StepDownMidCommitRetires(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-step-down");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        await using var committer = Elected(registry, "n2", gateway);

        var promoted = await committer.PromoteAsync(2UL, cancellationToken);
        var retired = await committer.RetireAsync(cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((promoted, retired)).IsEqualTo((false, true));
        _ = await Assert.That(committer.Tenure).IsNull();
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((1UL, 0UL));

        // A write that reaches the retired committer is refused like the write gate refuses it, never as an internal fault.
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
    }

    /// <summary>A read that finds its entry expired in a group this node leads without authority is refused as an expiration still pending.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredReadWithoutAuthorityIsPending(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-expired-read");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await using var committers = ElectedSet(registry, new ScriptedGateway());
        var inner = new StubCache();
        await inner.SetEntryAsync(NewOperationId(), CacheName, "b", new NodeCacheEntry<object?>("v", 1, DateTime.UtcNow.AddMinutes(-1)), cancellationToken);
        var cache = new ReplicatedCache(inner, committers);

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<object?>?>(cache.GetEntryAsync(CacheName, "b", cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ExpirationPendingDetail));
    }

    /// <summary>A committed entry that could not be applied keeps the coordinator and the lease: the retirement is refused until it applies.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetireWaitsForCommittedApply(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-retire-apply");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, "n2", "n1");
        await using var committer = Elected(registry, "n2", new ScriptedGateway(), applier, cache);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        GrantAuthority(registry, "n2", 2UL);
        cache.OnApplied = static () => throw new IOException("memory refused the apply");
        _ = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        var refused = await committer.RetireAsync(cancellationToken);
        var leaseKept = !applier.DriverLease.TryEnterPass();
        cache.OnApplied = null;
        var retired = await committer.RetireAsync(cancellationToken);

        _ = await Assert.That((refused, leaseKept, retired, applier.AppliedIndex)).IsEqualTo((false, true, true, 2UL));
    }

    /// <summary>Once the log moved past the led term, verification is blocked and the promotion is never authorized in the old term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherLogTermBlocksLeader(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-higher-term");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        await using var committer = Elected(registry, "n2", gateway);
        _ = await committer.PromoteAsync(2UL, cancellationToken);

        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        gateway.Set("n2", FollowerMode.Match);
        gateway.Set("n3", FollowerMode.Match);

        _ = await Assert.That(await committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Blocked);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsFalse();
    }

    /// <summary>
    /// The set of led groups grows with a promotion and shrinks with a retirement, and a write reaches a led group only while the election
    /// state reports local authority: before the leader-term entry authorizes it, it is refused retryably, after a higher term as stale-term,
    /// and once another leader is known as a stale owner.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WritesNeedLocalAuthority(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-write-gate");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = await TermAsync(registry, "n2", 2UL, cancellationToken);
        await using var committers = ElectedSet(registry, new ScriptedGateway());
        var state = registry.StateFor("n2");
        state.SetElectionDriven(true);

        var authorized = await committers.PromoteAsync("n2", 2UL, cancellationToken);
        var led = committers.Leads("n2");
        var beforeGrant = NodeExceptionAssert.For<RpcException>().Throws(committers, static set => set.ForKey(CacheName, "b"));
        _ = state.BecomeLeader(2UL);
        _ = state.GrantAuthority(2UL);
        var granted = committers.ForKey(CacheName, "b").GroupId;
        state.ObserveHigherTerm(3UL);
        var deposed = committers.FindAuthorized("n2");
        var staleTerm = NodeExceptionAssert.For<RpcException>().Throws(committers, static set => set.ForKey(CacheName, "b"));
        state.BecomeFollower(3UL, false);
        state.ObserveLeaderContact("n3", 3UL);
        var otherLeader = NodeExceptionAssert.For<RpcException>().Throws(committers, static set => set.ForKey(CacheName, "b"));
        var retired = await committers.RetireAsync("n2", cancellationToken);

        _ = await Assert.That((authorized, led, granted, retired, committers.Leads("n2"))).IsEqualTo((true, true, "n2", true, false));
        _ = await Assert.That(deposed).IsNull();
        _ = await Assert.That((beforeGrant.StatusCode, beforeGrant.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That((staleTerm.StatusCode, staleTerm.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That(otherLeader.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(otherLeader.Status.Detail).Contains("'n3'");
        _ = await Assert.That(committers.Promotions!.TryRead(out var promotion)).IsTrue();
        _ = await Assert.That((promotion.Committer.GroupId, promotion.Tenure.IsCancellationRequested)).IsEqualTo(("n2", true));
    }

    /// <summary>The readiness service verifies a group promoted at runtime, which authorizes its leader, and its loop ends with the leadership.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadinessFollowsPromotedGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-elected-readiness");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        _ = await TermAsync(registry, "n2", 2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind);
        gateway.Set("n3", FollowerMode.Behind);
        await using var committers = ElectedSet(registry, gateway);
        var log = new EventRecordingLogger();
        using var service = new ReplicaGroupReadinessService(committers, log, TimeProvider.System);

        await service.StartAsync(cancellationToken);
        bool first;
        bool authorized;
        bool retired;
        try
        {
            first = await committers.PromoteAsync("n2", 2UL, cancellationToken);
            await log.WhenLoggedAsync(VerificationCompleteEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            authorized = await committers.PromoteAsync("n2", 2UL, cancellationToken);
            retired = await committers.RetireAsync("n2", cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That((first, authorized, retired)).IsEqualTo((false, true, true));
        _ = await Assert.That(service.ExecuteTask?.IsCompletedSuccessfully).IsTrue().Because("A host stop ends every leadership loop normally.");
    }

    private static ReplicaGroupCommitter Elected(
        ReplicaGroupRegistry registry,
        string groupId,
        ScriptedGateway gateway,
        ReplicaGroupApplier? applier = null,
        StubCache? cache = null)
    {
        var local = cache ?? new StubCache();
        return CreateElectedCommitter(registry, groupId, gateway, local, applier ?? new ReplicaGroupApplier(local, NullLogger.Instance, groupId, "n1"));
    }

    /// <summary>Hands the election state of a group the authority the driver grants once the leader-term entry is committed.</summary>
    /// <param name="registry">The registry serving the group.</param>
    /// <param name="groupId">The group.</param>
    /// <param name="term">The led term.</param>
    private static void GrantAuthority(ReplicaGroupRegistry registry, string groupId, ulong term)
    {
        var state = registry.StateFor(groupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(term);
        _ = state.GrantAuthority(term);
    }

    private static ReplicaGroupCommitters ElectedSet(ReplicaGroupRegistry registry, ScriptedGateway gateway) =>
        new(groupId => Elected(registry, groupId, gateway), new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")), "n1", Owners(), TimeProvider.System);

    private static async Task<List<ReplicaLogRecord>> NoopsAsync(IFollowerLog log, CancellationToken cancellationToken)
    {
        var read = await log.ReadEntriesAsync(1, 64, cancellationToken);
        var noops = new List<ReplicaLogRecord>(read.Entries.Count);
        foreach (var entry in read.Entries)
        {
            var record = ReplicaLogCodec.Decode(entry.Payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The log entry must decode."));
            if (string.Equals(record.MutationKind, ReplicaMutationKinds.LeaderNoop, StringComparison.Ordinal))
                noops.Add(record);
        }

        return noops;
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
}
