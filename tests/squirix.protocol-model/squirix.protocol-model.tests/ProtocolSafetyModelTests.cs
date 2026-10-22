using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class ProtocolSafetyModelTests
{
    private const int SingleEntryTerm = 1;
    private const int NewerTerm = 2;
    private const int ReplicaCount = 3;
    private const int LogRulesMaxTerm = 3;
    private const int LogRulesMaxEntries = 2;
    private const int LogRulesMaxInFlight = 1;
    private const int LogRulesStateBudget = 50_000;

    [Test]
    public async Task EntrySurvivesFutureLeaderSelection()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallCommit(), BrokenMode.None);
        _ = await Assert.That(result.FixedPointReached).IsTrue();
        _ = await Assert.That(result.Violation).IsNull();
    }

    [Test]
    public async Task MembershipElectsAtMostOneLeaderPerTerm()
    {
        var result = ExploreRunner.Run(ExploreProfile.SmallElection(), BrokenMode.None);
        _ = await Assert.That(result.FixedPointReached).IsTrue();
        _ = await Assert.That(result.Violation).IsNull();
        _ = await Assert.That(result.StatesVisited > 1).IsTrue();
    }

    /// <summary>
    /// An entry of an earlier term committed without an entry of the leader's term is lost to a later leader: the checker sees it from the
    /// state, as a committed entry the newest leader does not hold.
    /// </summary>
    [Test]
    public async Task OldTermEntryNeedsCurrentTermCommit()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallLogRules(), BrokenMode.None);
        _ = await Assert.That(safe.FixedPointReached).IsTrue();
        _ = await Assert.That(safe.Violation).IsNull();

        var broken = ExploreRunner.Run(ExploreProfile.EarlierTermLogRules(), BrokenMode.CurrentTermCommit);
        _ = await Assert.That(broken.Violation).IsNotNull();
        _ = await Assert.That(broken.Violation.Invariant).IsEqualTo("LeaderCompleteness");
    }

    /// <summary>
    /// A vote that compares the length of the logs before the term of their last entries elects a leader without a committed entry; the
    /// correct order holds in the same bounds, explored to their fixed point by the full profile.
    /// </summary>
    [Test]
    public async Task VoteComparesLastTermFirst()
    {
        var broken = ExploreRunner.Run(ExploreProfile.EarlierTermLogRules(), BrokenMode.LogOrder);
        _ = await Assert.That(broken.Violation).IsNotNull();
        _ = await Assert.That(broken.Violation.Invariant).IsEqualTo("LeaderCompleteness");
    }

    /// <summary>
    /// A member that lost its disk and is repaired with the term ceiling of a majority never helps elect a second leader in a term it voted
    /// in before the loss; repaired without the ceiling, it does.
    /// </summary>
    [Test]
    public async Task RepairAfterDiskLossNeedsTermCeiling()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallDiskLoss(), BrokenMode.None);
        _ = await Assert.That(safe.Violation).IsNull();
        _ = await Assert.That(safe.FixedPointReached).IsTrue();

        var broken = ExploreRunner.Run(ExploreProfile.SmallDiskLoss(), BrokenMode.RepairCeiling);
        _ = await Assert.That(broken.Violation).IsNotNull();
        _ = await Assert.That(broken.Violation.Invariant).IsEqualTo("ElectionSafety");
    }

    [Test]
    public async Task QuorumReadRequiresCurrentTermMajority()
    {
        var safe = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.None);
        _ = await Assert.That(safe.FixedPointReached).IsTrue();
        _ = await Assert.That(safe.Violation).IsNull();

        var broken = ExploreRunner.Run(ExploreProfile.SmallRead(), BrokenMode.ReadIndex);
        _ = await Assert.That(broken.Violation).IsNotNull();
        _ = await Assert.That(broken.Violation.Invariant).IsEqualTo("ReadIndex");
    }

    /// <summary>
    /// Every rise of a node's commit index is the node's own leader rule (an entry of its current term at or below the new index) or the repair
    /// of a disk-loss copy by a leader; no node learns a commit index from another leader's decision without a message.
    /// </summary>
    [Test]
    public async Task CommitIndexRisesOnlyByLeaderRule()
    {
        var unreducedCommit = CountUnexplainedRises(ExploreProfile.SmallCommit(false));
        var logRules = CountUnexplainedRises(ExploreProfile.ForLogRules(LogRulesMaxTerm, LogRulesMaxEntries, LogRulesMaxInFlight, LogRulesStateBudget));

        _ = await Assert.That(unreducedCommit).IsEqualTo(0);
        _ = await Assert.That(logRules).IsEqualTo(0);
    }

    /// <summary>One node's commit index is a commit decision: an entry it covers is committed even when no other node has learned that.</summary>
    [Test]
    public async Task OneCommitIndexMakesEntryCommitted()
    {
        LogEntry[] committedEntry = [new(SingleEntryTerm, 1)];
        var state = ClusterState.CreateInitial(ReplicaCount).WithNodes(
        [
            new NodeState(0, NodeRole.Follower, SingleEntryTerm, -1, committedEntry, NodeRuntime.Create(1, 1, 0, 0, 0, false), false),
            new NodeState(1, NodeRole.Leader, NewerTerm, 1, [], NodeRuntime.Create(0, 0, 0b101, 0, 0, false), false),
            new NodeState(2, NodeRole.Follower, NewerTerm, 1, [], in NodeRuntime.Initial, false),
        ]);

        var violation = SafetyChecker.Check(state);

        _ = await Assert.That(violation).IsNotNull();
        _ = await Assert.That(violation.Invariant).IsEqualTo("LeaderCompleteness");
    }

    private static int CountUnexplainedRises(ExploreProfile profile)
    {
        var initial = ClusterState.CreateInitial(profile.ReplicaCount);
        var seen = new HashSet<string>(StringComparer.Ordinal) { initial.Fingerprint(false) };
        var queue = new Queue<ClusterState>();
        queue.Enqueue(initial);
        var unexplained = 0;
        while (queue.Count > 0 && seen.Count < profile.MaxStates)
        {
            var current = queue.Dequeue();
            foreach (var successor in CollectionsMarshal.AsSpan(ExploreRunner.NextStates(current, profile, BrokenMode.None)))
            {
                for (var i = 0; i < current.Nodes.Count; i++)
                {
                    if (successor.Nodes[i].CommitIndex > current.Nodes[i].CommitIndex && !IsOwnRuleOrRepair(current.Nodes[i], successor.Nodes[i]))
                        unexplained++;
                }

                if (seen.Add(successor.Fingerprint(false)))
                    queue.Enqueue(successor);
            }
        }

        return unexplained;
    }

    private static bool IsOwnRuleOrRepair(NodeState before, NodeState after)
    {
        if (before.Unseeded && !after.Unseeded)
            return true;

        if (after.Role != NodeRole.Leader)
            return false;

        for (var i = 0; i < after.LogEntries.Count; i++)
        {
            if (after.LogEntries[i].Term == after.CurrentTerm && after.LogEntries[i].Index <= after.CommitIndex)
                return true;
        }

        return false;
    }
}
