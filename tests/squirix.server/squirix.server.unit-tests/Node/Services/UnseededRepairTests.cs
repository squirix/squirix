using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A leader repairs a follower that lost its disk only after it read the term ceiling from a majority without that follower (#1126): with no
/// term above its own it transfers the state with the ceiling flag; with a higher term it steps down and sends nothing; with too few answers it
/// waits. Node n1 owns group n1; n3 lost its disk.
/// </summary>
public sealed class UnseededRepairTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private string OwnerDir => Path.Join(Dir, "owner");

    /// <summary>
    /// With n2 at the leader's term, n3 gets the state with the ceiling flag and is admitted; only n2 is asked for its term, and no entry is sent
    /// to n3 before the transfer.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CeilingRepairsUnseededFollower(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, cancellationToken);
        var statuses = new ScriptedStatusGateway();
        statuses.Set("n2", GroupMemberState.Complete, 1UL);
        var transfer = new ScriptedTransferGateway { OnEnd = () => leader.Gateway.Set("n3", FollowerMode.Behind, 3) };
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);

        var admitted = await leader.Committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, statuses, cancellationToken), cancellationToken);

        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(string.Join(',', transfer.Calls)).IsEqualTo("begin,snapshot,wipe,entries,end");
        _ = await Assert.That(transfer.Baseline?.CeilingChecked).IsTrue();
        _ = await Assert.That(string.Join(',', statuses.Calls)).IsEqualTo("n2");
        _ = await Assert.That(leader.Registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
    }

    /// <summary>A member at a term above the leader's stops the repair before anything is captured or sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermStopsRepair(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, cancellationToken);
        var statuses = new ScriptedStatusGateway();
        statuses.Set("n2", GroupMemberState.Complete, 2UL);
        var transfer = new ScriptedTransferGateway();
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);

        var admitted = await leader.Committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, statuses, cancellationToken), cancellationToken);

        _ = await Assert.That(admitted).IsFalse();
        _ = await Assert.That(transfer.Calls.IsEmpty).IsTrue();
        _ = await Assert.That(leader.Committer.TransferPin.IsHeld).IsFalse();
    }

    /// <summary>
    /// Without an answer from a member that kept its term, the ceiling does not hold and nothing is sent: n2 down, refusing, or itself without its
    /// disk, which has no term to tell.
    /// </summary>
    /// <param name="answer">How n2 answers the status call.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("down")]
    [Arguments("refused")]
    [Arguments("unseeded")]
    public async Task TooFewAnswersSendNothing(string answer, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, cancellationToken);
        var statuses = new ScriptedStatusGateway();
        if (string.Equals(answer, "refused", StringComparison.Ordinal))
            statuses.Refuse("n2", 1UL);
        else if (string.Equals(answer, "unseeded", StringComparison.Ordinal))
            statuses.Set("n2", GroupMemberState.Unseeded, 0UL);

        var transfer = new ScriptedTransferGateway();
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);

        var admitted = await leader.Committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, statuses, cancellationToken), cancellationToken);

        _ = await Assert.That(admitted).IsFalse();
        _ = await Assert.That(transfer.Calls.IsEmpty).IsTrue();
    }

    /// <summary>
    /// At five replicas the ceiling needs two of the three members other than the leader and the wiped one; a copy that lost its disk too is not
    /// counted, and the highest answer is reported.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FiveReplicasNeedTwoAnswers(CancellationToken cancellationToken)
    {
        string[] members = ["a", "b", "c", "d", "e"];
        var statuses = new ScriptedStatusGateway();
        statuses.Set("c", GroupMemberState.Complete, 3UL);
        statuses.Set("d", GroupMemberState.Incomplete, 4UL);
        var ceiling = new ReplicaTermCeiling(statuses, HangGuard);
        var header = new ReplicaRpcHeader("a", Fingerprint, 1UL, 4UL, "a", "a");

        var enough = await ceiling.ReadAsync(members, header, 1, 0, cancellationToken);
        statuses.Set("d", GroupMemberState.Unseeded, 0UL);
        var short1 = await ceiling.ReadAsync(members, header, 1, 0, cancellationToken);

        _ = await Assert.That((enough.Answers, enough.Needed, enough.Highest, enough.Suffices)).IsEqualTo((2, 2, 4UL, true));
        _ = await Assert.That((short1.Answers, short1.Suffices)).IsEqualTo((1, false));
        _ = await Assert.That(statuses.Calls).DoesNotContain("a");
        _ = await Assert.That(statuses.Calls).DoesNotContain("b");
    }

    /// <summary>At four replicas the ceiling needs both members other than the leader and the wiped one: with the leader they are three of four.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FourReplicasNeedTwoAnswers(CancellationToken cancellationToken)
    {
        string[] members = ["a", "b", "c", "d"];
        var statuses = new ScriptedStatusGateway();
        statuses.Set("c", GroupMemberState.Complete, 3UL);
        var ceiling = new ReplicaTermCeiling(statuses, HangGuard);
        var header = new ReplicaRpcHeader("a", Fingerprint, 1UL, 4UL, "a", "a");

        var one = await ceiling.ReadAsync(members, header, 1, 0, cancellationToken);
        statuses.Set("d", GroupMemberState.Incomplete, 4UL);
        var two = await ceiling.ReadAsync(members, header, 1, 0, cancellationToken);

        _ = await Assert.That((one.Answers, one.Needed, one.Suffices)).IsEqualTo((1, 2, false));
        _ = await Assert.That((two.Answers, two.Needed, two.Suffices)).IsEqualTo((2, 2, true));
    }

    /// <summary>
    /// The repair-ceiling counterexample of the protocol model: a node that voted in a term and lost its disk could vote again, so a stale leader
    /// of that term would repair it and meet a second leader of the same term. Here n1 leads group n2 in term 2, n3 lost its disk after it voted
    /// in term 3, and n2 holds term 3. The stale leader observes term 3, which steps it down, and sends n3 nothing.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The group log n2 is not open.</exception>
    [Test]
    public async Task StaleLeaderStepsDownBeforeRepair(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, Groups, null, cancellationToken);
        if (!registry.TryGetLog("n2", out var log))
            throw new InvalidOperationException("The group log n2 is not open.");

        _ = await log.ObserveTermAsync(2UL, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Match);
        gateway.Set("n3", FollowerMode.Unseeded);
        var local = new StubCache();
        await using var committer = CreateElectedCommitter(registry, "n2", gateway, local, new ReplicaGroupApplier(local, NullLogger.Instance, "n2", "n1"));
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(cancellationToken);
        var statuses = new ScriptedStatusGateway();
        statuses.Set("n2", GroupMemberState.Complete, 3UL);
        var transfer = new ScriptedTransferGateway();

        _ = await committer.CatchUpFollowersAsync(Reporter("n2"), await TransfersAsync(transfer, statuses, cancellationToken), cancellationToken);

        _ = await Assert.That(transfer.Calls.IsEmpty).IsTrue();
        _ = await Assert.That(registry.StateFor("n2").HighestObservedTerm).IsEqualTo(3UL);
    }

    private static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    private static ReplicaCatchUpReporter Reporter(string groupId = GroupId) => new(groupId, NullLogger.Instance, null);

    private static async Task<ReplicaStateTransfers> TransfersAsync(ScriptedTransferGateway gateway, ScriptedStatusGateway statuses, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var cache = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        for (var i = 1; i <= 3; i++)
            await cache.SetAsync(new CacheKey("cache", $"k{i}"), new NodeCacheEntry<object?>($"v{i}"), cancellationToken);

        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(GroupId);
        return new ReplicaStateTransfers(gateway, new ReplicaGroupEntries(cache, locator.Instance()), Durable(), new ReplicaTransferOptions(), NullLogger<ReplicaStateTransfers>.Instance, null)
        {
            TermCeiling = new ReplicaTermCeiling(statuses, HangGuard),
        };
    }

    /// <summary>Opens the owner of group n1 with three committed and applied writes; n2 holds the log and n3 lost its disk.</summary>
    /// <param name="registry">The open registry of the owner.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The leader.</returns>
    private static async Task<Leader> OpenLeaderAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Match);
        gateway.Set("n3", FollowerMode.Unseeded);
        var leader = new Leader(registry, gateway);
        try
        {
            for (var i = 1; i <= 3; i++)
                await leader.Committer.CommitSetAsync(NewOperationId(), "cache", $"k{i}", Entry($"k{i}"), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            return leader;
        }
        catch
        {
            await leader.DisposeAsync();
            throw;
        }
    }

    /// <summary>The owner of a group with its committer and the follower transport.</summary>
    private sealed class Leader : IAsyncDisposable
    {
        internal Leader(ReplicaGroupRegistry registry, ScriptedGateway gateway)
        {
            Registry = registry;
            Committer = CreateCommitter(registry, gateway);
            Gateway = gateway;
        }

        internal ReplicaGroupCommitter Committer { get; }

        internal ScriptedGateway Gateway { get; }

        internal ReplicaGroupRegistry Registry { get; }

        public ValueTask DisposeAsync() => Committer.DisposeAsync();
    }
}
