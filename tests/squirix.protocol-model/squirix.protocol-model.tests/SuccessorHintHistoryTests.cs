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

    /// <summary>Walks the profile and returns the first state whose path named two successors in one term.</summary>
    /// <param name="broken">The injected fault.</param>
    /// <returns>The witness, or <see langword="null" /> when no reachable state has one.</returns>
    /// <remarks>
    /// The walk keeps one emission history per reachable state, keyed by the same fingerprint the explorer dedups on, so
    /// a state reached by two paths keeps the history of the first path that arrived. That is enough for a witness and
    /// it is the same convention the explorer uses, which is what keeps this walk's reachable set the explorer's.
    /// </remarks>
    private static SuccessorWitness? FindSecondSuccessor(BrokenMode broken)
    {
        var profile = ExploreProfile.SmallTransferOneMoreTerm();
        var initial = ClusterState.CreateInitial(profile.ReplicaCount);
        Dictionary<string, Dictionary<(int Sender, int Term), SortedSet<int>>> history = [with(StringComparer.Ordinal)];
        history[initial.Fingerprint(profile.SymmetryReduce)] = [];
        var queue = new Queue<ClusterState>();
        queue.Enqueue(initial);

        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            var currentHistory = history[current.Fingerprint(profile.SymmetryReduce)];
            var successors = ExploreRunner.NextStates(current, profile, broken);
            for (var i = 0; i < successors.Count; i++)
            {
                var next = successors[i];
                var nextHistory = Extend(currentHistory, current, next);

                var witness = WitnessFor(next, nextHistory);
                if (witness != null)
                    return witness;

                var nextKey = next.Fingerprint(profile.SymmetryReduce);
                if (history.ContainsKey(nextKey))
                    continue;

                history[nextKey] = nextHistory;
                queue.Enqueue(next);
            }
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

            var inFlight = 0;
            for (var i = 0; i < state.Messages.Count; i++)
            {
                var message = state.Messages[i];
                if (message.Kind == MsgKind.TransferHint && message.From == key.Sender && message.Term == key.Term)
                    inFlight++;
            }

            if (inFlight == 0)
                return new SuccessorWitness(state, key.Sender, key.Term, targets);
        }

        return null;
    }

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
