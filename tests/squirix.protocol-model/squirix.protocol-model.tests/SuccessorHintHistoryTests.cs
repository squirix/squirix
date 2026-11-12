using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>The successor rule must still see a hint after that hint has left the message list.</summary>
/// <remarks>
/// <para>
/// <see cref="ProtocolPolicyChecker.HintNamesOneSuccessor" /> used to count only the hints that remained in
/// <see cref="ClusterState.Messages" />. A hint is delivered or dropped before the next step, so that count answered "how
/// many hints are in flight" rather than "how many successors did this retirement name". Emitting a hint, letting it
/// leave, and then naming a different successor in the same term was invisible to the check even though it is exactly
/// the event the policy is about.
/// </para>
/// <para>
/// The evidence in the first test is built from the transitions, not from the model's own bookkeeping: the walk records
/// every hint it sees emitted and asks the checker about the states it reaches, so the test cannot be satisfied by the
/// model agreeing with itself. The witness is taken only in a state with no transfer hint in flight, because a hint that
/// is still in flight would be caught by the neighbouring rules and the reading would be about a different defect.
/// </para>
/// <para>
/// The sound rule is the control: it produces a hint only from a retirement, and a retirement turns the leader into a
/// follower in that term, so it cannot emit a second one.
/// </para>
/// </remarks>
public sealed class SuccessorHintHistoryTests
{
    /// <summary>Evidence outlives the message but not the right to name another successor.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The two states below carry the same record and differ only in whether its sender still leads the term. Pruning is
    /// what keeps the fault from multiplying states across terms, and the sound rule depends on it: there the retirement
    /// demotes the leader in the same step, so a retirement leaves no lasting label behind.
    /// </remarks>
    [Test]
    public async Task EvidenceOutlivesTheMessage()
    {
        ModelHintEmission[] record = [new(0, 1, 1, -1)];
        var stillLeading = ClusterState.CreateInitial(3)
            .WithNodes(Members(0, NodeRole.Leader, 1))
            .With(hints: record);
        var alreadySteppedDown = ClusterState.CreateInitial(3)
            .WithNodes(Members(0, NodeRole.Follower, 1))
            .With(hints: record);

        _ = await Assert.That(stillLeading.HintEmissions.Count).IsEqualTo(1)
            .Because("a leader that has not left can still name another successor in that term");
        _ = await Assert.That(alreadySteppedDown.HintEmissions.Count).IsEqualTo(0)
            .Because("a member that no longer leads the term cannot name anything in it");
        _ = await Assert.That(ProtocolPolicyChecker.Check(alreadySteppedDown)).IsNull();
    }

    /// <summary>Two terms are two retirements, and a different member in one term is one retirement naming two.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task HintsInDifferentTermsAreDistinct()
    {
        IReadOnlyList<ModelHintEmission> none = [];
        var firstTerm = ModelHintHistory.With(none, 0, 1, 1);
        var acrossTerms = ModelHintHistory.With(firstTerm, 0, 2, 2);

        _ = await Assert.That(acrossTerms.Count).IsEqualTo(2);
        _ = await Assert.That(acrossTerms[0].SecondSuccessor < 0).IsTrue();
        _ = await Assert.That(acrossTerms[1].SecondSuccessor < 0).IsTrue()
            .Because("each term named one successor, whatever the other term did");

        var sameTerm = ModelHintHistory.With(firstTerm, 0, 1, 2);
        _ = await Assert.That(sameTerm.Count).IsEqualTo(1);
        _ = await Assert.That(sameTerm[0].SecondSuccessor).IsEqualTo(2)
            .Because("the same term naming a different member is the event the policy is about");
    }

    /// <summary>A state the checker calls violating never shares its exploration key with a clean one.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// <para>
    /// The exploration decides whether to look at a state by its key, and it does that before it asks the policy checker.
    /// A violating state whose key equals a clean state's key is therefore discarded as already seen, and the violation it
    /// carries is never recorded — the state retains the evidence and the search still loses it.
    /// </para>
    /// <para>
    /// The states here come from the model's own transitions, not from a hand-built pair: the clean keys are the ones
    /// <see cref="ExploreRunner.ReachableStates" /> returns and the checker calls clean, and the violating keys are on
    /// successors <see cref="ExploreRunner.NextStates" /> produces from them. Nothing is fabricated and no history is
    /// removed, so a collision found this way is one the real search can take.
    /// </para>
    /// </remarks>
    [Test]
    public async Task ReachableViolationNeverSharesCleanKey()
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        const BrokenMode broken = BrokenMode.TransferHint;
        var reachable = ExploreRunner.ReachableStates(profile, broken);

        HashSet<string> clean = [with(StringComparer.Ordinal)];
        for (var i = 0; i < reachable.Count; i++)
        {
            if (ProtocolPolicyChecker.Check(reachable[i]) == null)
                _ = clean.Add(reachable[i].Fingerprint(profile.SymmetryReduce));
        }

        var violating = 0;
        string? collision = null;
        for (var i = 0; i < reachable.Count && collision == null; i++)
        {
            var successors = ExploreRunner.NextStates(reachable[i], profile, broken);
            for (var k = 0; k < successors.Count; k++)
            {
                var violation = ProtocolPolicyChecker.Check(successors[k]);
                if (violation == null
                    || !string.Equals(violation.Policy, ProtocolPolicyChecker.HintNamesOneSuccessor, StringComparison.Ordinal))
                    continue;

                violating++;
                var key = successors[k].Fingerprint(profile.SymmetryReduce);
                if (!clean.Contains(key))
                    continue;
                collision = violation.Detail;
                break;
            }
        }

        _ = await Assert.That(violating).IsGreaterThan(0)
            .Because("the fault names two successors in one term, so the walk must produce violating successors");
        _ = await Assert.That(collision).IsNull()
            .Because(
                "the violating successor sits on a key the exploration already holds for a clean state, so the search " +
                "discards it before the checker sees it: " + (collision ?? string.Empty));
    }

    /// <summary>Evidence that already names a second successor survives the retirement that demotes its sender.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// <para>
    /// The sequence: a leader names one successor while it still serves, that hint leaves the message list, and the leader
    /// then retires in the same term naming a different successor. The retirement is also the step that demotes the
    /// sender, and the state it produces was built by pruning the record against the new roles — so the record that held
    /// the violation was dropped by the same step that established it, and the checker saw neither rule fire.
    /// </para>
    /// <para>
    /// The witness asserts the demotion explicitly: a sequence where the sender still led the term would be the case the
    /// message list already covers, and a reading taken there would say nothing about this one.
    /// </para>
    /// </remarks>
    [Test]
    public async Task RetirementKeepsEarlierSuccessorEvidence()
    {
        var witness = FindRetirementAfterEarlierSuccessor(BrokenMode.TransferHint);

        _ = await Assert.That(witness).IsNotNull()
            .Because("the fault lets a leader that is not leaving name a successor, and the retirement names another");

        _ = await Assert.That(witness!.State.Nodes[witness.Sender].Role).IsNotEqualTo(NodeRole.Leader)
            .Because("the retirement demoted the sender, which is the step that used to drop the evidence");

        var violation = ProtocolPolicyChecker.Check(witness.State);

        _ = await Assert.That(violation).IsNotNull()
            .Because(
                "node " + witness.Sender.ToString(CultureInfo.InvariantCulture) + " named " +
                witness.First.ToString(CultureInfo.InvariantCulture) + " and " +
                witness.Second.ToString(CultureInfo.InvariantCulture) + " in term " +
                witness.Term.ToString(CultureInfo.InvariantCulture) +
                ", the first hint has left the message list, and the second named a different member");
        _ = await Assert.That(violation!.Policy).IsEqualTo(ProtocolPolicyChecker.HintNamesOneSuccessor);
        _ = await Assert.That(violation.Detail).Contains("did not name one");
    }

    /// <summary>The same successor named twice is one successor, whatever the network does with the copies.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task RetransmissionIsNotADifferentSuccessor()
    {
        IReadOnlyList<ModelHintEmission> none = [];
        var once = ModelHintHistory.With(none, 0, 1, 1);
        var twice = ModelHintHistory.With(once, 0, 1, 1);

        _ = await Assert.That(once.Count).IsEqualTo(1);
        _ = await Assert.That(twice.Count).IsEqualTo(1).Because("naming the same successor again says nothing new");
        _ = await Assert.That(twice[0].SecondSuccessor < 0).IsTrue()
            .Because("both hints name member 1, so the retirement still named one successor");
    }

    /// <summary>A second successor named in one term is reachable under the fault, and the checker must report it.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task SecondSuccessorIsReported()
    {
        var witness = FindSecondSuccessor(BrokenMode.TransferHint);

        _ = await Assert.That(witness).IsNotNull()
            .Because("the fault lets a leader that is not leaving emit again, so the sequence should be reachable");

        var violation = ProtocolPolicyChecker.Check(witness!.State);
        _ = await Assert.That(violation).IsNotNull()
            .Because(
                "node " + witness.Sender.ToString(CultureInfo.InvariantCulture) + " named successors " +
                JoinSuccessors(witness.Successors) + " in term " +
                witness.Term.ToString(CultureInfo.InvariantCulture) +
                " and no transfer hint is in flight, so nothing in the message list carries the second successor");
        _ = await Assert.That(violation!.Policy).IsEqualTo(ProtocolPolicyChecker.HintNamesOneSuccessor);
        _ = await Assert.That(violation.Detail).Contains("did not name one");
    }

    /// <summary>The sound rule never names two successors in one term, so the same walk finds nothing there.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// This is the control for the test above: if the sound rule could produce the sequence too, the witness would be
    /// about the profile rather than about the fault, and a check that fired on it would be firing on a legal execution.
    /// </remarks>
    [Test]
    public async Task SoundRuleNeverNamesTwoSuccessors()
    {
        var witness = FindSecondSuccessor(BrokenMode.None);

        _ = await Assert.That(witness).IsNull()
            .Because("a retirement demotes the leader in the same term, so one term can name one successor");
    }

    /// <summary>Takes the first hint out of flight, then looks for a retirement that names another successor.</summary>
    /// <param name="emitted">The state after the fault's hint was sent.</param>
    /// <param name="profile">The exploration profile.</param>
    /// <param name="broken">The injected fault.</param>
    /// <param name="actor">The member that named the successor.</param>
    /// <param name="term">The term it named.</param>
    /// <param name="first">The successor that hint named, or -1 when the step put none in flight.</param>
    /// <returns>The witness, or <see langword="null" />.</returns>
    private static RetirementWitness? DriveDeparture(ClusterState emitted, ExploreProfile profile, BrokenMode broken, int actor, int term, int first)
    {
        if (first < 0 || !StillLeads(emitted, actor, term))
            return null;

        var departed = ExploreRunner.NextStates(emitted, profile, broken);
        for (var d = 0; d < departed.Count; d++)
        {
            var witness = DriveRetirement(departed[d], profile, broken, actor, term, first);
            if (witness != null)
                return witness;
        }

        return null;
    }

    /// <summary>Retires a leader that has named one successor and keeps the state when it names a different one.</summary>
    /// <param name="departed">The state with no hint from the actor left in flight.</param>
    /// <param name="profile">The exploration profile.</param>
    /// <param name="broken">The injected fault.</param>
    /// <param name="actor">The member that is retiring.</param>
    /// <param name="term">The term it retires in.</param>
    /// <param name="first">The successor it named earlier in that term.</param>
    /// <returns>The witness, or <see langword="null" />.</returns>
    private static RetirementWitness? DriveRetirement(ClusterState departed, ExploreProfile profile, BrokenMode broken, int actor, int term, int first)
    {
        if (!StillLeads(departed, actor, term) || HasHintInFlight(departed, actor, term))
            return null;

        var retired = ExploreRunner.NextStates(departed, profile, broken);
        for (var x = 0; x < retired.Count; x++)
        {
            var second = NamedBy(departed, retired[x], actor, term);
            if (StillLeads(retired[x], actor, term) || second < 0 || second == first)
                continue;

            return new RetirementWitness(retired[x], actor, term, first, second);
        }

        return null;
    }

    /// <summary>Drives emit, depart and retire from one seed, or returns null when the seed cannot reach all three.</summary>
    /// <param name="seed">The state the sequence starts from.</param>
    /// <param name="profile">The exploration profile.</param>
    /// <param name="broken">The injected fault.</param>
    /// <param name="actor">The member that names the successors.</param>
    /// <returns>The witness, or <see langword="null" />.</returns>
    private static RetirementWitness? DriveThreeSteps(ClusterState seed, ExploreProfile profile, BrokenMode broken, int actor)
    {
        var term = seed.Nodes[actor].CurrentTerm;
        if (HasHintInFlight(seed, actor, term))
            return null;

        var emitted = ExploreRunner.NextStates(seed, profile, broken);
        for (var e = 0; e < emitted.Count; e++)
        {
            var witness = DriveDeparture(emitted[e], profile, broken, actor, term, NamedBy(seed, emitted[e], actor, term));
            if (witness != null)
                return witness;
        }

        return null;
    }

    /// <summary>Copies the path's emissions and adds the hint this transition put in flight.</summary>
    /// <param name="previous">The emissions along the path to <paramref name="before" />.</param>
    /// <param name="before">The state the transition started from.</param>
    /// <param name="after">The state it produced.</param>
    /// <returns>The emissions along the path, including this one.</returns>
    private static Dictionary<(int Sender, int Term), SortedSet<int>> Extend(
        Dictionary<(int Sender, int Term), SortedSet<int>> previous,
        ClusterState before,
        ClusterState after)
    {
        Dictionary<(int Sender, int Term), SortedSet<int>>? extended = null;
        for (var i = 0; i < after.Messages.Count; i++)
        {
            var message = after.Messages[i];
            if (message.Kind != MsgKind.TransferHint || message.Id < before.NextMessageId)
                continue;

            if (extended == null)
            {
                extended = [];
                foreach (var (key, targetsAlreadyNamed) in previous)
                    extended[key] = [.. targetsAlreadyNamed];
            }

            if (!extended.TryGetValue((message.From, message.Term), out var targets))
            {
                targets = [];
                extended[(message.From, message.Term)] = targets;
            }

            _ = targets.Add(message.To);
        }

        return extended ?? previous;
    }

    /// <summary>Drives the fault's hint, its departure, and a retirement naming a different successor.</summary>
    /// <param name="broken">The injected fault.</param>
    /// <returns>The witness, or <see langword="null" /> when no reachable leader reaches the three steps.</returns>
    /// <remarks>
    /// The seed is a reachable state rather than a hand-built one, so the sequence is one the model admits. Seeds are
    /// taken in walk order and the first that completes the three steps wins; the budget bounds the search without
    /// changing what a witness means, because a null answer is only ever read together with the assertion above it.
    /// </remarks>
    private static RetirementWitness? FindRetirementAfterEarlierSuccessor(BrokenMode broken)
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var reachable = ExploreRunner.ReachableStates(profile, broken);
        var seeds = 0;
        for (var r = 0; r < reachable.Count && seeds < 64; r++)
        {
            var state = reachable[r];
            for (var actor = 0; actor < state.Nodes.Count; actor++)
            {
                if (state.Nodes[actor].Role != NodeRole.Leader)
                    continue;

                seeds++;
                var witness = DriveThreeSteps(state, profile, broken, actor);
                if (witness != null)
                    return witness;
            }
        }

        return null;
    }

    /// <summary>Walks the profile and returns the first state whose path named two successors in one term.</summary>
    /// <param name="broken">The injected fault.</param>
    /// <returns>The witness, or <see langword="null" /> when no reachable state has one.</returns>
    /// <remarks>
    /// The walk deduplicates by the same fingerprint as the explorer, so
    /// a state reached by two paths keeps the history of the first path that arrived. That is enough for a witness and
    /// it is the same convention the explorer uses, which is what keeps this walk's reachable set the explorer's.
    /// </remarks>
    private static SuccessorWitness? FindSecondSuccessor(BrokenMode broken)
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var initial = ClusterState.CreateInitial(profile.ReplicaCount);
        HashSet<string> seen = [with(StringComparer.Ordinal), initial.Fingerprint(profile.SymmetryReduce)];
        var queue = new Queue<(ClusterState State, Dictionary<(int Sender, int Term), SortedSet<int>> History)>();
        queue.Enqueue((initial, []));

        while (queue.Count > 0)
        {
            var (current, currentHistory) = queue.Dequeue();
            var successors = ExploreRunner.NextStates(current, profile, broken);
            for (var i = 0; i < successors.Count; i++)
            {
                var next = successors[i];
                var nextHistory = Extend(currentHistory, current, next);

                var witness = WitnessFor(next, nextHistory);
                if (witness != null)
                    return witness;

                var nextKey = next.Fingerprint(profile.SymmetryReduce);
                if (!seen.Add(nextKey))
                    continue;

                queue.Enqueue((next, nextHistory));
            }
        }

        return null;
    }

    /// <summary>Checks whether the member has a hint in flight in the given term.</summary>
    /// <param name="state">The state.</param>
    /// <param name="sender">The member that sent them.</param>
    /// <param name="term">The term they name.</param>
    /// <returns>Whether a matching hint is in flight.</returns>
    private static bool HasHintInFlight(ClusterState state, int sender, int term)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message.Kind == MsgKind.TransferHint && message.From == sender && message.Term == term)
                return true;
        }

        return false;
    }

    /// <summary>Renders the successors a witness named, so a failure names the members and not just a count.</summary>
    /// <param name="successors">The successors named.</param>
    /// <returns>The comma-separated member ids.</returns>
    private static string JoinSuccessors(SortedSet<int> successors)
    {
        var sb = new StringBuilder();
        foreach (var successor in successors)
        {
            if (sb.Length > 0)
                _ = sb.Append(',');

            _ = sb.Append(successor.ToString(CultureInfo.InvariantCulture));
        }

        return sb.ToString();
    }

    /// <summary>Builds three members, one of them holding the given role and term.</summary>
    /// <param name="actor">The member that holds the role.</param>
    /// <param name="role">The role it holds.</param>
    /// <param name="term">The term every member is in.</param>
    /// <returns>The member list.</returns>
    private static List<NodeState> Members(int actor, NodeRole role, int term)
    {
        var nodes = new List<NodeState>(3);
        for (var id = 0; id < 3; id++)
        {
            var nodeRole = id == actor ? role : NodeRole.Follower;
            var votedFor = id == actor && role == NodeRole.Leader ? id : -1;
            nodes.Add(new NodeState(id, nodeRole, term, votedFor, [], NodeRuntime.Create(0, 0, 0, 0, 0, false), false));
        }

        return nodes;
    }

    /// <summary>Returns the successor a transition named for one member and term, or -1 when it named none.</summary>
    /// <param name="before">The state the transition started from.</param>
    /// <param name="after">The state it produced.</param>
    /// <param name="sender">The member that names the successor.</param>
    /// <param name="term">The term it names.</param>
    /// <returns>The named member, or -1.</returns>
    private static int NamedBy(ClusterState before, ClusterState after, int sender, int term)
    {
        for (var i = 0; i < after.Messages.Count; i++)
        {
            var message = after.Messages[i];
            if (message.Kind == MsgKind.TransferHint && message.From == sender && message.Term == term
                && message.Id >= before.NextMessageId)
                return message.To;
        }

        return -1;
    }

    /// <summary>Returns whether one member still leads one term in a state.</summary>
    /// <param name="state">The state.</param>
    /// <param name="actor">The member.</param>
    /// <param name="term">The term.</param>
    /// <returns><see langword="true" /> when it leads that term.</returns>
    private static bool StillLeads(ClusterState state, int actor, int term)
    {
        var node = state.Nodes[actor];
        return node.Role == NodeRole.Leader && node.CurrentTerm == term;
    }

    /// <summary>Returns the witness when this state's path named two successors in one term with no hint in flight.</summary>
    /// <param name="state">The state reached.</param>
    /// <param name="history">The emissions along the path to it.</param>
    /// <returns>The witness, or <see langword="null" />.</returns>
    /// <remarks>
    /// The in-flight test is what makes this witness about the missing history rather than about a hint the checker can
    /// already see: with a hint still queued the message list holds the evidence, and the reading would be attributed to
    /// a rule that was never the one at fault.
    /// </remarks>
    private static SuccessorWitness? WitnessFor(
        ClusterState state,
        Dictionary<(int Sender, int Term), SortedSet<int>> history)
    {
        foreach (var (key, targets) in history)
        {
            if (targets.Count < 2)
                continue;

            if (!HasHintInFlight(state, key.Sender, key.Term))
                return new SuccessorWitness(state, key.Sender, key.Term, targets);
        }

        return null;
    }

    /// <summary>A state a retirement reached after the same member had already named another successor in that term.</summary>
    /// <param name="State">The state the retirement produced.</param>
    /// <param name="Sender">The member that named both successors and was demoted by the retirement.</param>
    /// <param name="Term">The term both namings happened in.</param>
    /// <param name="First">The successor named while the sender still served.</param>
    /// <param name="Second">The different successor the retirement named.</param>
    private sealed record RetirementWitness(
        ClusterState State,
        int Sender,
        int Term,
        int First,
        int Second);

    /// <summary>A state whose path named more than one successor in one term, with none of them still in flight.</summary>
    /// <param name="State">The state reached.</param>
    /// <param name="Sender">The member that named them.</param>
    /// <param name="Term">The term they were named in.</param>
    /// <param name="Successors">The distinct successors named.</param>
    private sealed record SuccessorWitness(
        ClusterState State,
        int Sender,
        int Term,
        SortedSet<int> Successors);
}
