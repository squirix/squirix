using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
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
    private static readonly ReadOnlyMemory<byte> Fingerprint = ReadOnlyMemory<byte>.Of(9);

    /// <summary>An accepted heartbeat names its leader; a pre-vote within the election timeout is refused, and answered once it expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteRefusedWhileLeaderIsLive(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var dir = new TempDirectory("squirix-follower-election-contact");
        await using var registry = await OpenRegistryAsync(dir, time, cancellationToken);
        var follower = new ReplicaFollower(registry);
        var ballot = new ElectionVoteRequest("n3", 8UL, 0UL, 0UL);

        _ = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch([], "n2", 7UL, 0UL, 0UL, 0UL), cancellationToken);
        var refused = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);
        time.Advance(registry.Election.ElectionTimeout);
        var answered = await follower.PreVoteAsync(GroupId, Fingerprint, 1UL, ballot, cancellationToken);

        _ = await Assert.That((refused.Granted, refused.RefusalCode, refused.CurrentTerm)).IsEqualTo((false, RefusalCodes.LeaderContact, 7UL));
        _ = await Assert.That(answered.Granted).IsTrue();
        _ = await Assert.That(registry.StateFor(GroupId).TryGetKnownLeader(out var leader, out var term)).IsTrue();
        _ = await Assert.That((leader, term)).IsEqualTo(("n2", 7UL));
    }

    /// <summary>A stale-term append is no leader contact: it neither names a leader nor delays the election.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleAppendIsNoLeaderContact(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-stale");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry);
        _ = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), cancellationToken);
        var state = registry.StateFor(GroupId);
        var reset = state.ElectionResetTimestamp();

        var stale = await follower.AppendAsync(GroupId, Fingerprint, 1UL, new FollowerBatch([], "n2", 4UL, 0UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That(stale.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That(state.TryGetKnownLeader(out _, out _)).IsFalse();
        _ = await Assert.That(state.ElectionResetTimestamp()).IsEqualTo(reset);
    }

    /// <summary>A vote the log made durable at a higher term reaches the election state, so a leader of an older term cannot gain authority.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteTermDeposesOlderLeader(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-election-vote");
        await using var registry = await OpenRegistryAsync(dir, new FakeTimeProvider(), cancellationToken);
        var follower = new ReplicaFollower(registry);
        var state = registry.StateFor(GroupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);

        var vote = await follower.RequestVoteAsync(GroupId, Fingerprint, 1UL, new ElectionVoteRequest("n3", 5UL, 0UL, 0UL), cancellationToken);

        _ = await Assert.That((vote.Granted, vote.CurrentTerm)).IsEqualTo((true, 5UL));
        _ = await Assert.That(state.HighestObservedTerm).IsEqualTo(5UL);
        _ = await Assert.That(state.GrantAuthority(2UL)).IsFalse();
        _ = await Assert.That(state.ElectionResetTimestamp()).IsNotNull();
    }

    private static async Task<ReplicaGroupRegistry> OpenRegistryAsync(TempDirectory dir, TimeProvider time, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, [GroupId], 3, Fingerprint, 1UL, NullLoggerFactory.Instance)
        {
            Election = new ElectionTimerOptions { JitterSeed = 1UL },
            ElectionClock = time,
        };
        await registry.OpenAsync(cancellationToken);
        return registry;
    }
}
