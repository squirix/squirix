using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Follower repair ordering, eligibility, and lifecycle integration.</summary>
public sealed class ReplicaRepairTests : NodeIntegrationTestBase
{
    private const string GroupId = "repair-group";

    /// <summary>A predecessor-term conflict at the commit boundary quarantines storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PredecessorConflictAtCommitQuarantines(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-diverged");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "committed"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 2UL, "diverged"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(1UL, cancellationToken)).Success).IsTrue();

        var reconcile = await log.ReconcileTailAsync(2UL, 2UL, 2UL, cancellationToken);

        _ = await Assert.That(reconcile.Success).IsFalse();
        _ = await Assert.That(reconcile.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(reconcile.Quarantined).IsTrue();
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>A repair instruction at or below the committed boundary quarantines storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QuarantineOnCommittedBoundary(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-committed");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "committed"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(1UL, cancellationToken)).Success).IsTrue();

        var reconcile = await log.ReconcileTailAsync(1UL, 0UL, 1UL, cancellationToken);

        _ = await Assert.That(reconcile.Success).IsFalse();
        _ = await Assert.That(reconcile.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(reconcile.Quarantined).IsTrue();
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>Reconciling an already-absent tail succeeds without side effects.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReconcileNoOpAtTailSucceeds(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-noop");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "committed"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "tail"), cancellationToken)).Success).IsTrue();

        var reconcile = await log.ReconcileTailAsync(3UL, 1UL, 1UL, cancellationToken);

        _ = await Assert.That(reconcile.Success).IsTrue();
        _ = await Assert.That(reconcile.Quarantined).IsFalse();
        _ = await Assert.That(reconcile.ReleasedReservations).IsEqualTo(0);

        var mismatch = await log.ReconcileTailAsync(3UL, 2UL, 1UL, cancellationToken);

        _ = await Assert.That(mismatch.Success).IsFalse();
        _ = await Assert.That(mismatch.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(mismatch.Quarantined).IsFalse();
    }

    /// <summary>A repair instruction from a stale leader term is refused without truncating.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReconcileRejectsStaleLeaderTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-stale-term");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "committed"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "stale"), cancellationToken)).Success).IsTrue();

        var reconcile = await log.ReconcileTailAsync(2UL, 1UL, 0UL, cancellationToken);

        _ = await Assert.That(reconcile.Success).IsFalse();
        _ = await Assert.That(reconcile.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That(reconcile.Quarantined).IsFalse();
        _ = await Assert.That(log.Readiness).IsNotEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await log.GetUncommittedTailAsync(cancellationToken)).Count).IsEqualTo(2);
    }

    /// <summary>A repair instruction with a stale predecessor term is refused without truncating or quarantine.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReconcileRejectsStalePredecessorTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-term");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "committed"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "stale"), cancellationToken)).Success).IsTrue();

        var reconcile = await log.ReconcileTailAsync(2UL, 2UL, 1UL, cancellationToken);

        _ = await Assert.That(reconcile.Success).IsFalse();
        _ = await Assert.That(reconcile.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(reconcile.Quarantined).IsFalse();
        _ = await Assert.That(log.Readiness).IsNotEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await log.GetUncommittedTailAsync(cancellationToken)).Count).IsEqualTo(2);
    }

    /// <summary>A zero repair index is caller-bug input and is refused without quarantine.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReconcileZeroIndexWithoutQuarantine(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-zero");
        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "committed"), cancellationToken)).Success).IsTrue();

        var reconcile = await log.ReconcileTailAsync(0UL, 0UL, 1UL, cancellationToken);

        _ = await Assert.That(reconcile.Success).IsFalse();
        _ = await Assert.That(reconcile.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(reconcile.Quarantined).IsFalse();
        _ = await Assert.That(log.Readiness).IsNotEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>A restarted stale replica remains excluded until its durable progress exactly reaches the leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartedReplicaRemainsExcluded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-restart");
        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
        {
            await log.OpenAsync(cancellationToken);
            _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "one"), cancellationToken)).Success).IsTrue();
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        var fingerprint = new byte[] { 1, 2, 3 };
        var observed = Progress(status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex, 1UL, fingerprint, 7U);
        var expected = Progress(2UL, 0UL, 0UL, 2UL, fingerprint, 9U);
        var eligibility = new ReplicaEligibility(3);
        var quorum = new ReplicaCommitQuorum(3, 2UL, eligibility);

        _ = await Assert.That(eligibility.TryMarkReady(0, in expected, in expected)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkCatchingUp(1, in observed)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkReady(1, in observed, in expected)).IsFalse();
        _ = await Assert.That(quorum.FindCommitIndex(0UL, 2UL)).IsEqualTo(0UL);

        _ = await Assert.That(eligibility.TryMarkReady(1, in expected, in expected)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0UL, 2UL)).IsEqualTo(2UL);
    }

    /// <summary>A durable repair truncation releases only the removed tail reservation and survives restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncationReleasesReservation(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-truncate");
        var faults = new ArmableFlushFaultHooks(static () => new IOException("simulated crash after durable repair truncation"));

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), faults))
        {
            await log.OpenAsync(cancellationToken);
            _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "committed"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "stale"), cancellationToken)).Success).IsTrue();
            _ = await Assert.That((await log.AdvanceCommitAsync(1UL, cancellationToken)).Success).IsTrue();
            _ = log.Idempotency.Reserve("client", "pending", [4], GroupRecordKind.UserMutation, 2UL, 1UL);
            faults.Arm();

            var reconcile = log.ReconcileTailAsync(2UL, 1UL, 1UL, cancellationToken);
            _ = await NodeAsyncAssert.ThrowsAsync<IOException>(reconcile);
            _ = await Assert.That(log.Idempotency.Lookup("client", "pending", [4], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await reopened.OpenAsync(cancellationToken);
        var restarted = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That(restarted.LastLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(await reopened.GetUncommittedTailAsync(cancellationToken)).IsEmpty();
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader",
        term,
        index - 1UL,
        index == 1UL ? 0UL : 1UL,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(Entry(index, term, payload)));

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
