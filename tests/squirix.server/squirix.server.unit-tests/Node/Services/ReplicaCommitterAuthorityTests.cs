using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
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

    /// <summary>A higher term seen after the write was routed refuses it with stale-term before the append.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeposedLeaderRefusesBeforeAppend(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-deposed");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var (committer, log) = await LeadAsync(registry, cancellationToken);
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
        var (committer, log) = await LeadAsync(registry, cancellationToken);
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
        var (committer, log) = await LeadAsync(registry, cancellationToken);
        await using var owned = committer;
        _ = await log.ObserveTermAsync(3UL, cancellationToken);
        var last = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
        _ = await Assert.That(refused.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-term");
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(last);
    }

    /// <summary>A leader whose election state has not granted authority yet refuses retryably.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PromotionPendingRefusesUnavailable(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-authority-pending");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = await TermAsync(registry, cancellationToken);
        await using var committer = Create(registry);
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
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The authorized committer, owned by the caller, and the group log.</returns>
    private static async Task<(ReplicaGroupCommitter Committer, IFollowerLog Log)> LeadAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var log = await TermAsync(registry, cancellationToken);
        var committer = Create(registry);
        _ = await Assert.That(await committer.PromoteAsync(2UL, cancellationToken)).IsTrue();
        var state = registry.StateFor(Group);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        _ = await Assert.That(state.GrantAuthority(2UL)).IsTrue();
        await committer.CommitSetAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        return (committer, log);
    }

    private static ReplicaGroupCommitter Create(ReplicaGroupRegistry registry)
    {
        var cache = new StubCache();
        return CreateElectedCommitter(registry, Group, new ScriptedGateway(), cache, new ReplicaGroupApplier(cache, NullLogger.Instance, Group, "n1"));
    }

    private static async Task<IFollowerLog> TermAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog(Group, out var log))
            throw new InvalidOperationException($"The group log {Group} is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        return log;
    }
}
