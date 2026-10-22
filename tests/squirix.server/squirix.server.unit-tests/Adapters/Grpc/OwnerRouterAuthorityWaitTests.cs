using System;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A trusted internal owner RPC that reaches a node elected right before it waits for that node's own authority instead of being refused (#1095).</summary>
[Immutable]
public sealed class OwnerRouterAuthorityWaitTests
{
    private const string Self = "node-a";

    /// <summary>The leader wait of the routers <see cref="OwnerRouterRerouteTests.CreateRouter" /> builds.</summary>
    private static readonly TimeSpan LeaderWait = TimeSpan.FromSeconds(2);

    /// <summary>
    /// A forwarded call that reaches a node elected right before it, whose leader-term entry is not committed yet, waits for the node's own
    /// authority within the leader wait and then runs here: the client gets no refusal to retry.
    /// </summary>
    [Test]
    public async Task InternalCallWaitsForAuthority()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var table = new FakeLeaderTable(Self, default) { LedWithoutAuthority = true, LedTerm = 3, GrantOnWait = true };
        var attempts = new OwnerRouterRerouteTests.Attempts();

        var answer = await attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, true, clock));

        _ = await Assert.That(answer).IsEqualTo(OwnerRouterRerouteTests.Attempts.Local);
        _ = await Assert.That(string.Join(',', attempts.Targets)).IsEqualTo(OwnerRouterRerouteTests.Attempts.Local);
        _ = await Assert.That(table.AuthorityWaits.Count == 1 && table.AuthorityWaits[0] == LeaderWait).IsTrue();
    }

    /// <summary>A forwarded call whose node does not get authority within the wait is refused as before, without running.</summary>
    [Test]
    public async Task InternalCallWithoutAuthorityIsRefused()
    {
        var table = new FakeLeaderTable(Self, default) { LedWithoutAuthority = true, LedTerm = 3 };
        var attempts = new OwnerRouterRerouteTests.Attempts();

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempts.RunAsync(OwnerRouterRerouteTests.CreateRouter(table, true, TimeProvider.System)));

        _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
        _ = await Assert.That(table.AuthorityWaits.Count).IsEqualTo(1);
        _ = await Assert.That(attempts.Targets.Count).IsEqualTo(0);
    }

    /// <summary>A node that leads without authority but already saw a higher term refuses a forwarded call as stale at once, without waiting.</summary>
    [Test]
    public async Task InternalCallAfterHigherTermIsNotHeld()
    {
        var table = new FakeLeaderTable(Self, default) { LedWithoutAuthority = true, LedTerm = 3, HighestObservedTerm = 4, GrantOnWait = true };
        var attempts = new OwnerRouterRerouteTests.Attempts();
        var router = OwnerRouterRerouteTests.CreateRouter(table, true, TimeProvider.System);

        var failure = NodeExceptionAssert.For<RpcException>().Throws((Router: router, Attempts: attempts), static s => _ = s.Attempts.RunAsync(s.Router));

        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(table.AuthorityWaits.Count).IsEqualTo(0);
        _ = await Assert.That(attempts.Targets.Count).IsEqualTo(0);
    }
}
