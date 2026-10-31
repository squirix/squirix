using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Whether a copy may take a state transfer, decided from its durable status and the commit index it announced.</summary>
[Immutable]
public sealed class ReplicaTransferAdmissionTests : ServerUnitTestBase
{
    private const string GroupId = "grp-hint";
    private const ulong Term = 1UL;

    /// <summary>The admission of a transfer compares the baseline with the hint as well as with the durable commit index.</summary>
    [Test]
    public async Task AdmissionCountsTheHint()
    {
        var header = new ReplicaTransferHeader(GroupId, "transfer", "leader-1", Term);
        var baseline = new ReplicaTransferBaseline(3UL, Term, 0, 0U, ReadOnlyMemory<byte>.Empty, 0UL);
        var view = new GroupLeaderView(false, false, false, 0, 0, default);
        var announced = Status(0UL, 4UL);
        var durable = Status(4UL, 0UL);
        var unrelated = Status(0UL, 0UL);

        _ = await Assert.That(ReplicaTransferAdmission.Refusal(in header, in baseline, in announced, in view)).IsEqualTo(RefusalCodes.NotReady);
        _ = await Assert.That(ReplicaTransferAdmission.Refusal(in header, in baseline, in durable, in view)).IsEqualTo(RefusalCodes.NotReady);
        _ = await Assert.That(ReplicaTransferAdmission.Refusal(in header, in baseline, in unrelated, in view)).IsNull();
    }

    private static FollowerLogStatus Status(ulong commitIndex, ulong hint) =>
        new(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, Term, string.Empty, 4UL, Term, commitIndex, 0UL, FollowerLogReadiness.Ready, GroupMemberState.Complete) { CommitHint = hint };
}
