using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The hold at the start of an election driver names the state of the copy that keeps the node from leading.</summary>
[Immutable]
public sealed class ElectionHoldTests : ServerUnitTestBase
{
    /// <summary>An incomplete copy is denied as incomplete, an unseeded one as unseeded, and a log that is not ready is held silently.</summary>
    /// <param name="incomplete">Whether the copy is incomplete; otherwise it is unseeded.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task IncompleteIsNotReportedAsUnseeded(bool incomplete)
    {
        var status = StatusOf(incomplete ? GroupMemberState.Incomplete : GroupMemberState.Unseeded, FollowerLogReadiness.Ready);

        var outcome = ElectionHold.OnStart(in status);

        var denial = incomplete ? FailoverDenial.Incomplete : FailoverDenial.Unseeded;
        _ = await Assert.That(outcome).IsEqualTo(new ElectionOutcome(ElectionEvent.Denied, 3UL, denial));
    }

    /// <summary>A complete copy that is ready starts, and a log that is not ready is held without a denial.</summary>
    [Test]
    public async Task CompleteCopyStartsAndUnreadyLogWaits()
    {
        var complete = StatusOf(GroupMemberState.Complete, FollowerLogReadiness.Ready);
        var unready = StatusOf(GroupMemberState.Incomplete, FollowerLogReadiness.Failed);

        _ = await Assert.That(ElectionHold.OnStart(in complete)).IsNull();
        _ = await Assert.That(ElectionHold.OnStart(in unready)).IsEqualTo(new ElectionOutcome(ElectionEvent.None, 3UL));
    }

    private static FollowerLogStatus StatusOf(GroupMemberState state, FollowerLogReadiness readiness) =>
        new("g", ReadOnlyMemory<byte>.Empty, 1UL, 3UL, string.Empty, 0UL, 0UL, 0UL, 0UL, readiness, state);
}
