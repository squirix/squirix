using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
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

        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in leader, Fingerprint, 1);

        _ = await Assert.That(eligibility.CanCountInWriteQuorum(0)).IsTrue();
    }

    /// <summary>
    /// A follower that answered a start probe from its log counts as in contact with the elected leader; an unreachable or refusing one
    /// does not.
    /// </summary>
    /// <returns>An asynchronous operation.</returns>
    [Test]
    public async Task ProbeAnswersCountAsContact()
    {
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 5UL };
        var state = new ReplicaGroupState(5, options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        time.Advance(options.ElectionTimeout);

        ReplicaReadinessProbe.RecordContacts(state, [default, new(ReplicaProbeKind.Refused, 0), new(ReplicaProbeKind.Unreachable, 0), default, default], 2UL);
        var silent = state.HasQuorumContact(0, options.ElectionTimeout);
        ReplicaReadinessProbe.RecordContacts(state, [default, new(ReplicaProbeKind.Accepted, 3), new(ReplicaProbeKind.LogMismatch, 1), default, default], 2UL);

        _ = await Assert.That(silent).IsFalse();
        _ = await Assert.That(state.HasQuorumContact(0, options.ElectionTimeout)).IsTrue();
    }
}
