using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Xunit;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Follower repair ordering, eligibility, and lifecycle integration.</summary>
public sealed class ReplicaRepairTests : NodeIntegrationTestBase
{
    private const string GroupId = "repair-group";

    /// <summary>A restarted stale replica remains excluded until its durable progress exactly reaches the leader.</summary>
    [Fact(DisplayName = "Squirix.Server.IntegrationTests.Cluster.Replication.ReplicaRepairTests.RestartedReplicaCannotJoinQuorumBeforeCatchUp")]
    public async Task RestartedReplicaRemainsExcluded()
    {
        using var dir = new TempDirectory("squirix-repair-restart");
        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
        {
            await log.OpenAsync(DefaultCancellationToken);
            Assert.True((await log.AppendAsync(Append(1UL, 1UL, "one"), DefaultCancellationToken)).Success);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await reopened.OpenAsync(DefaultCancellationToken);
        var status = await reopened.GetStatusAsync(DefaultCancellationToken);
        var fingerprint = new byte[] { 1, 2, 3 };
        var observed = Progress(status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex, 1UL, fingerprint, 7U);
        var expected = Progress(2UL, 0UL, 0UL, 2UL, fingerprint, 9U);
        var eligibility = new ReplicaEligibility(3);
        var quorum = new ReplicaCommitQuorum(3, 2UL, eligibility);

        Assert.True(eligibility.TryMarkReady(0, in expected, in expected));
        Assert.True(eligibility.TryMarkCatchingUp(1, in observed));
        Assert.False(eligibility.TryMarkReady(1, in observed, in expected));
        Assert.Equal(0UL, quorum.FindCommitIndex(0UL, 2UL));

        Assert.True(eligibility.TryMarkReady(1, in expected, in expected));
        Assert.Equal(2UL, quorum.FindCommitIndex(0UL, 2UL));
    }

    /// <summary>A durable repair truncation releases only the removed tail reservation and survives restart.</summary>
    [Fact(DisplayName = "Squirix.Server.IntegrationTests.Cluster.Replication.ReplicaRepairTests.TailTruncationReleasesPendingReservation")]
    public async Task TruncationReleasesReservation()
    {
        using var dir = new TempDirectory("squirix-repair-truncate");
        var faults = new ArmableFlushFaultHooks(static () => new IOException("simulated crash after durable repair truncation"));

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), faults))
        {
            await log.OpenAsync(DefaultCancellationToken);
            Assert.True((await log.AppendAsync(Append(1UL, 1UL, "committed"), DefaultCancellationToken)).Success);
            Assert.True((await log.AppendAsync(Append(2UL, 1UL, "stale"), DefaultCancellationToken)).Success);
            Assert.True((await log.AdvanceCommitAsync(1UL, DefaultCancellationToken)).Success);
            _ = log.Idempotency.Reserve("client", "pending", new byte[] { 4 }, GroupRecordKind.UserMutation, 2UL, 1UL);
            faults.Arm();

            var reconcile = log.ReconcileTailAsync(2UL, 1UL, DefaultCancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<IOException>(reconcile);
            Assert.Equal(GroupIdempotencyLookup.Miss, log.Idempotency.Lookup("client", "pending", new byte[] { 4 }, out _));
            Assert.Equal(FollowerLogReadiness.Failed, log.Readiness);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await reopened.OpenAsync(DefaultCancellationToken);
        var restarted = await reopened.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(1UL, restarted.LastLogIndex);
        Assert.Empty(await reopened.GetUncommittedTailAsync(DefaultCancellationToken));
    }

    /// <summary>A repair instruction with a stale predecessor term is refused without truncating or quarantine.</summary>
    [Fact]
    public async Task ReconcileRejectsStalePredecessorTerm()
    {
        using var dir = new TempDirectory("squirix-repair-term");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(DefaultCancellationToken);
        Assert.True((await log.AppendAsync(Append(1UL, 1UL, "committed"), DefaultCancellationToken)).Success);
        Assert.True((await log.AppendAsync(Append(2UL, 1UL, "stale"), DefaultCancellationToken)).Success);

        var reconcile = await log.ReconcileTailAsync(2UL, 2UL, DefaultCancellationToken);

        Assert.False(reconcile.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, reconcile.RefusalCode);
        Assert.False(reconcile.Quarantined);
        Assert.NotEqual(FollowerLogReadiness.Failed, log.Readiness);
        Assert.Equal(2, (await log.GetUncommittedTailAsync(DefaultCancellationToken)).Count);
    }

    /// <summary>A zero repair index is caller-bug input and is refused without quarantine.</summary>
    [Fact]
    public async Task ReconcileZeroIndexWithoutQuarantine()
    {
        using var dir = new TempDirectory("squirix-repair-zero");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(DefaultCancellationToken);
        Assert.True((await log.AppendAsync(Append(1UL, 1UL, "committed"), DefaultCancellationToken)).Success);

        var reconcile = await log.ReconcileTailAsync(0UL, 0UL, DefaultCancellationToken);

        Assert.False(reconcile.Success);
        Assert.Equal(FollowerLogRefusal.LogMismatch, reconcile.RefusalCode);
        Assert.False(reconcile.Quarantined);
        Assert.NotEqual(FollowerLogReadiness.Failed, log.Readiness);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader",
        term,
        index - 1UL,
        index == 1UL ? 0UL : 1UL,
        0UL,
        new ReadOnlyMemory<FollowerLogEntry>([Entry(index, term, payload)]));

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));

    private static ReplicaProgress Progress(ulong matchIndex, ulong commitIndex, ulong appliedIndex, ulong lastTerm, byte[] fingerprint, uint checksum) => new(
        matchIndex + 1UL,
        matchIndex,
        commitIndex,
        appliedIndex,
        lastTerm,
        fingerprint,
        1UL,
        checksum);
}
