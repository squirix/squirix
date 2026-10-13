using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>What the leader does with a follower that answers incomplete: it stays out of the write quorum and counts as a contact of the leader.</summary>
[Immutable]
public sealed class ReplicaIncompleteFollowerTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly ElectionTimerOptions Options = new() { ElectionTimeout = TimeSpan.FromMilliseconds(500), JitterSeed = 5UL };

    /// <summary>An incomplete reply from the follower's log is a contact for the leader's quorum check, as an append or a probe answer; an unseeded one is not.</summary>
    /// <param name="kind">How the answer reached the leader.</param>
    [Test]
    [Arguments("append")]
    [Arguments("probe")]
    public async Task IncompleteReplyIsContact(string kind)
    {
        var time = new FakeTimeProvider();
        var state = new ReplicaGroupState(3, Options, time);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        time.Advance(Options.ElectionTimeout);

        state.RecordFollowerReply(1, new FollowerLogAppendResult(false, RefusalCodes.Unseeded, 0UL, 0UL));
        var unseeded = state.HasQuorumContact(0, Options.ElectionTimeout);
        if (string.Equals(kind, "append", StringComparison.Ordinal))
            state.RecordFollowerReply(1, new FollowerLogAppendResult(false, RefusalCodes.Incomplete, 0UL, 0UL));
        else
            ReplicaReadinessProbe.RecordContacts(state, [default, new(ReplicaProbeKind.Incomplete, 0), default], 2UL);

        _ = await Assert.That(unseeded).IsFalse();
        _ = await Assert.That(state.HasQuorumContact(0, Options.ElectionTimeout)).IsTrue();
    }

    /// <summary>An incomplete verdict takes a ready slot, and a slot with verified progress, out of the write quorum and leaves both catching up.</summary>
    [Test]
    public async Task IncompleteVerdictEndsReadiness()
    {
        var eligibility = new ReplicaEligibility(3);
        var progress = new ReplicaProgress(4, 3, 3, 0, 1, Fingerprint, 1, 0);
        var leader = new FollowerLogStatus("n1", Fingerprint, 1, 1, string.Empty, 3, 1, 1, 0, FollowerLogReadiness.Ready, GroupMemberState.Complete);
        _ = await Assert.That(eligibility.TryMarkReady(1, in progress, in progress)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkCatchingUp(2, in progress)).IsTrue();
        var counted = eligibility.CanCountInWriteQuorum(1);

        ReplicaReadinessProbe.ApplyAll(eligibility, 0, [default, new(ReplicaProbeKind.Incomplete, 0), new(ReplicaProbeKind.Incomplete, 0)], in leader, Fingerprint, 1, null);

        _ = await Assert.That(counted).IsTrue();
        _ = await Assert.That((eligibility.StateFor(1), eligibility.StateFor(2))).IsEqualTo((ReplicaParticipantState.CatchingUp, ReplicaParticipantState.CatchingUp));
        _ = await Assert.That((eligibility.CanCountInWriteQuorum(1), eligibility.CanCountInWriteQuorum(2))).IsEqualTo((false, false));
    }

    /// <summary>A write commits on the leader and the healthy follower while the other follower answers incomplete, and the incomplete slot stays out of the quorum.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteCommitsWithoutIncompleteFollower(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-incomplete-follower-commit");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Incomplete);
        await using var committer = CreateCommitter(registry, gateway);

        await committer.CommitSetAsync(NewOperationId(), "cache", "k1", new NodeCacheEntry<object?> { Value = "v1", Version = 1 }, cancellationToken);

        var eligibility = registry.EligibilityFor("n1");
        _ = await Assert.That((eligibility.StateFor(1), eligibility.CanCountInWriteQuorum(1), eligibility.CanCountInWriteQuorum(2))).IsEqualTo((ReplicaParticipantState.CatchingUp, false, true));
        _ = registry.TryGetLog("n1", out var log);
        _ = await Assert.That((await log!.GetStatusAsync(cancellationToken)).CommitIndex).IsGreaterThan(0UL);
    }
}
