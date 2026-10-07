using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader-side readiness verdicts of <see cref="ReplicaReadinessProbe" />.</summary>
[Immutable]
public sealed class ReplicaReadinessProbeTests
{
    private const string GroupId = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>The leader's own slot is verified from its durable log even while that log carries an uncommitted tail.</summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task LeaderWithTailIsMarkedReady()
    {
        var eligibility = new ReplicaEligibility(3);
        var leader = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready);

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, in leader, Fingerprint, 1);

        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }
}
