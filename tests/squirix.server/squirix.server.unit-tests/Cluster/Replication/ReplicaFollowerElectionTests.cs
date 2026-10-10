using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>What the follower RPC paths post to the election state of a group, and the pre-vote refusal that state decides.</summary>
[Immutable]
public sealed class ReplicaFollowerElectionTests : ServerUnitTestBase
{
    private const string GroupId = "n1";
    private const string OutsiderId = "n9";
    private static readonly ReadOnlyMemory<byte> Fingerprint = ReadOnlyMemory<byte>.Of(9);

    /// <summary>An accepted heartbeat names its leader; a pre-vote within the election timeout is refused, and answered once it expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteRefusedWhileLeaderIsLive(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var dir = new TempDirectory("squirix-follower-election-contact");
        await using var registry = await OpenRegistryAsync(dir, time, cancellationToken);
        registry.StateFor(GroupId).SetElectionDriven(true);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var ballot = new ElectionVoteRequest("n3", 8UL, 0UL, 0UL);

        _ = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch([], "n2", 7UL, 0UL, 0UL, 0UL), cancellationToken);
        var refused = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);
        var named = registry.StateFor(GroupId).ReadRoute().Known;
        time.Advance(registry.Election.ElectionTimeout);
        var answered = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);

        _ = await Assert.That((refused.Granted, refused.RefusalCode, refused.CurrentTerm)).IsEqualTo((false, RefusalCodes.LeaderContact, 7UL));
        _ = await Assert.That(answered.Granted).IsTrue();
        _ = await Assert.That(named).IsEqualTo(new LeaderRoute("n2", 7UL));
        _ = await Assert.That(registry.StateFor(GroupId).ReadRoute().Known).IsEqualTo(default);
    }

    /// <summary>A group no election driver runs for answers a pre-vote from its log right after a leader contact, exactly as before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UndrivenGroupAnswersPreVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-undriven");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        _ = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch([], "n2", 7UL, 0UL, 0UL, 0UL), cancellationToken);

        var answered = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 8UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That(answered.Granted).IsTrue();
    }

    /// <summary>A pre-vote from a node outside the replica set is refused before the log, while a member's pre-vote is answered.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonMemberPreVoteRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-prevote-member");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry, RingMembers(registry));

        var refused = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest(OutsiderId, 8UL, 0UL, 0UL), cancellationToken);
        var answered = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 8UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((refused.Granted, refused.RefusalCode, refused.CurrentTerm)).IsEqualTo((false, RefusalCodes.NotMember, 0UL));
        _ = await Assert.That(answered.Granted).IsTrue();
    }

    /// <summary>A vote request from a node outside the replica set changes nothing: no term, no vote, no deposed leader, no election reset.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonMemberVoteRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-vote-member");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry, RingMembers(registry));
        var state = registry.StateFor(GroupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        var reset = state.ElectionResetTimestamp();
        var before = await follower.GetStatusAsync(GroupId, cancellationToken);

        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest(OutsiderId, 5UL, 0UL, 0UL), cancellationToken);
        var after = await follower.GetStatusAsync(GroupId, cancellationToken);

        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, RefusalCodes.NotMember, 0UL));
        _ = await Assert.That((after?.CurrentTerm, after?.VotedFor, after?.LastLogIndex)).IsEqualTo((before?.CurrentTerm, before?.VotedFor, before?.LastLogIndex));
        _ = await Assert.That(state.HighestObservedTerm).IsEqualTo(2UL);
        _ = await Assert.That(state.ElectionResetTimestamp()).IsEqualTo(reset);
        _ = await Assert.That(state.GrantAuthority(2UL)).IsTrue();
    }

    /// <summary>On a ring of four nodes with three replicas, a configured node outside the replica set of the group gets no vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RingNodeOutsideGroupRefused(CancellationToken cancellationToken)
    {
        string[] ring = ["n1", "n2", "n3", "n4"];
        var locator = new ReplicaGroupLocator(new PhysicalNodeRing(ring), 3);
        var replicaSet = new string[3];
        locator.GetReplicaGroup(GroupId, replicaSet);
        var outsider = ring[0];
        for (var i = 0; i < ring.Length; i++)
        {
            if (Array.IndexOf(replicaSet, ring[i]) < 0)
                outsider = ring[i];
        }

        using var dir = new TempDirectory("squirix-follower-election-ring-outsider");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry, new ReplicaMembership(locator, registry.GroupIds));

        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest(outsider, 5UL, 0UL, 0UL), cancellationToken);
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);

        _ = await Assert.That(Array.IndexOf(replicaSet, outsider)).IsLessThan(0);
        _ = await Assert.That((vote.Granted, vote.RefusalCode, vote.CurrentTerm)).IsEqualTo((false, RefusalCodes.NotMember, 0UL));
        _ = await Assert.That(status?.CurrentTerm).IsEqualTo(0UL);
    }

    /// <summary>A leader with authority refuses the pre-votes of its group: it is the live leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteRefusedWhileLeading(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-leading");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var state = registry.StateFor(GroupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        _ = state.GrantAuthority(2UL);

        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var refused = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 3UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((refused.Granted, refused.RefusalCode)).IsEqualTo((false, RefusalCodes.LeaderContact));
    }

    /// <summary>An accepted commit advance and an installed snapshot are leader contacts too, and their terms are observed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitAndSnapshotAreLeaderContacts(CancellationToken cancellationToken)
    {
        using var commitDir = new TempDirectory("squirix-follower-election-commit");
        using var snapshotDir = new TempDirectory("squirix-follower-election-snapshot");
        await using var committed = await OpenRegistryAsync(commitDir, new FakeTimeProvider(), cancellationToken);
        await using var installed = await OpenRegistryAsync(snapshotDir, new FakeTimeProvider(), cancellationToken);
        var snapshot = new GroupSnapshot(GroupId, Fingerprint, 1UL, 1UL, 1UL, 1UL, [], DateTime.UnixEpoch);

        var commit = await new ReplicaFollower(committed, RocksDoubles.CreateReplicaMembers()).AdvanceCommitAsync(GroupId, Fingerprint, 1UL, 0UL, 3UL, cancellationToken);
        var install = await new ReplicaFollower(installed, RocksDoubles.CreateReplicaMembers()).InstallSnapshotAsync(GroupId, Fingerprint, 1UL, snapshot, 4UL, cancellationToken);

        _ = await Assert.That((commit.Success, install.Success)).IsEqualTo((true, true));
        _ = await Assert.That(committed.StateFor(GroupId).HasRecentLeaderContact(TimeSpan.FromSeconds(1))).IsTrue();
        _ = await Assert.That(installed.StateFor(GroupId).HasRecentLeaderContact(TimeSpan.FromSeconds(1))).IsTrue();
        _ = await Assert.That((committed.StateFor(GroupId).HighestObservedTerm, installed.StateFor(GroupId).HighestObservedTerm)).IsEqualTo((3UL, 4UL));
    }

    /// <summary>A log mismatch still comes from the live leader: it names the leader, which then repairs the follower.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LogMismatchIsLeaderContact(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-mismatch");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);

        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var result = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch([], "n2", 3UL, 5UL, 1UL, 0UL), cancellationToken);

        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(registry.StateFor(GroupId).ReadRoute().Known).IsEqualTo(new LeaderRoute("n2", 3UL));
    }

    /// <summary>A stale-term append is no leader contact: it neither names a leader nor delays the election.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleAppendIsNoLeaderContact(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-stale");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        _ = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), cancellationToken);
        var state = registry.StateFor(GroupId);
        var reset = state.ElectionResetTimestamp();

        var stale = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch([], "n2", 4UL, 0UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That(stale.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That(state.ReadRoute().HasLeader).IsFalse();
        _ = await Assert.That(state.ElectionResetTimestamp()).IsEqualTo(reset);
    }

    /// <summary>A vote the log made durable at a higher term reaches the election state, so a leader of an older term cannot gain authority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteTermDeposesOlderLeader(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-vote");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var state = registry.StateFor(GroupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);

        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((vote.Granted, vote.CurrentTerm)).IsEqualTo((true, 5UL));
        _ = await Assert.That(state.HighestObservedTerm).IsEqualTo(5UL);
        _ = await Assert.That(state.GrantAuthority(2UL)).IsFalse();
        _ = await Assert.That(state.ElectionResetTimestamp()).IsNotNull();
    }

    /// <summary>A follower whose log adopted the fingerprint of one failover mode refuses every replication call of the other mode, unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherFailoverModeIsTopologyMismatch(CancellationToken cancellationToken)
    {
        var elected = ModeFingerprint(true);
        var unelected = ModeFingerprint(false);
        using var dir = new TempDirectory("squirix-follower-election-mode");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), elected, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var ballot = new ElectionVoteRequest("n3", 5UL, 0UL, 0UL);
        var before = await follower.GetStatusAsync(GroupId, cancellationToken);

        var append = await follower.AppendAsync(GroupId, unelected, 1UL, new FollowerBatch([], "n2", 3UL, 0UL, 0UL, 0UL), cancellationToken);
        var preVote = await follower.PreVoteAsync(GroupId, unelected, 1UL, ballot, cancellationToken);
        var vote = await follower.RequestVoteAsync(GroupId, unelected, 1UL, ballot, cancellationToken);
        var commit = await follower.AdvanceCommitAsync(GroupId, unelected, 1UL, 0UL, 3UL, cancellationToken);
        var snapshot = new GroupSnapshot(GroupId, unelected, 1UL, 1UL, 1UL, 1UL, [], DateTime.UnixEpoch);
        var install = await follower.InstallSnapshotAsync(GroupId, unelected, 1UL, snapshot, 3UL, cancellationToken);
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);

        _ = await Assert.That(unelected.AsSpan().SequenceEqual(elected)).IsFalse();
        _ = await Assert.That((append.Success, append.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch));
        _ = await Assert.That((preVote.Granted, preVote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch));
        _ = await Assert.That((vote.Granted, vote.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch));
        _ = await Assert.That((commit.Success, commit.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch));
        _ = await Assert.That((install.Success, install.RefusalCode)).IsEqualTo((false, FollowerLogRefusal.TopologyMismatch));
        _ = await Assert.That((status?.CurrentTerm, status?.VotedFor, status?.LastLogIndex)).IsEqualTo((before?.CurrentTerm, before?.VotedFor, before?.LastLogIndex));
        _ = await Assert.That(status?.CurrentTerm).IsEqualTo(0UL);
        _ = await Assert.That(registry.StateFor(GroupId).ReadRoute().HasLeader).IsFalse();
    }

    /// <summary>A cancellation that lands after the term step became durable does not undo the vote: it is granted and the election state learns the term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelAfterTermStepFinishesVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-cancel-step");
        var hooks = new CancelOnMetaWritten();
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), Fingerprint, new FollowerLogOptions { FaultHooks = hooks }, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        hooks.Arm(caller);

        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), caller.Token);
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);

        _ = await Assert.That(caller.IsCancellationRequested).IsTrue();
        _ = await Assert.That((vote.Granted, vote.CurrentTerm)).IsEqualTo((true, 5UL));
        _ = await Assert.That((status?.CurrentTerm, status?.VotedFor)).IsEqualTo((5UL, "n3"));
        _ = await Assert.That(registry.StateFor(GroupId).HighestObservedTerm).IsEqualTo(5UL);
    }

    /// <summary>A cancellation before the term step throws and leaves nothing durable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelBeforeTermStepLeavesNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-cancel-before");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        using var caller = new CancellationTokenSource();
        await caller.CancelAsync();

        var vote = follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), caller.Token);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(vote);
        var status = await follower.GetStatusAsync(GroupId, cancellationToken);
        _ = await Assert.That((status?.CurrentTerm, status?.VotedFor)).IsEqualTo((0UL, string.Empty));
    }

    private static byte[] ModeFingerprint(bool elected)
    {
        ServerPeer[] peers =
        [
            new() { NodeId = "n1", Uri = new Uri("https://127.0.0.1:6001") },
            new() { NodeId = "n2", Uri = new Uri("https://127.0.0.1:6002") },
            new() { NodeId = "n3", Uri = new Uri("https://127.0.0.1:6003") },
        ];
        var topology = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 3,
            AutomaticFailoverEnabled = elected,
            QuorumReadsEnabled = elected,
        };
        return [.. TopologyFingerprint.CreateFromTopology(topology, new MtlsOptions()).Bytes];
    }

    private static ReplicaMembership RingMembers(ReplicaGroupRegistry registry) =>
        new(new ReplicaGroupLocator(new PhysicalNodeRing(["n1", "n2", "n3"]), 3), registry.GroupIds);

    private static Task<ReplicaGroupRegistry> OpenRegistryAsync(TempDirectory dir, TimeProvider time, CancellationToken cancellationToken) =>
        OpenRegistryAsync(dir, time, Fingerprint, cancellationToken);

    private static Task<ReplicaGroupRegistry> OpenRegistryAsync(TempDirectory dir, TimeProvider time, ReadOnlyMemory<byte> fingerprint, CancellationToken cancellationToken) =>
        OpenRegistryAsync(dir, time, fingerprint, null, cancellationToken);

    private static async Task<ReplicaGroupRegistry> OpenRegistryAsync(
        TempDirectory dir,
        TimeProvider time,
        ReadOnlyMemory<byte> fingerprint,
        FollowerLogOptions? options,
        CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, fingerprint, 1UL, NullLoggerFactory.Instance, options)
        {
            Election = new ElectionTimerOptions { JitterSeed = 1UL },
            ElectionClock = time,
        };
        await registry.OpenAsync(cancellationToken);
        return registry;
    }

    /// <summary>Cancels a caller token when the next metadata write reaches the file, which is after the write of the term step began.</summary>
    [ThreadSafe]
    private sealed class CancelOnMetaWritten : IFollowerLogFaultHooks
    {
        private CancellationTokenSource? _armed;

        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
        }

        public void OnMetaWritten() => Interlocked.Exchange(ref _armed, null)?.Cancel();

        internal void Arm(CancellationTokenSource source) => Volatile.Write(ref _armed, source);
    }
}
