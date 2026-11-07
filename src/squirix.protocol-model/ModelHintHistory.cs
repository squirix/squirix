using System.Collections.Generic;

namespace Squirix.ProtocolModel;

/// <summary>Retains what a member named in a term, independently of the hints that are still in flight.</summary>
/// <remarks>
/// A hint is delivered or dropped before the next step, so the message list holds only what has not landed yet. Reading
/// that list answers "how many hints are in flight", while the policy asks "how many successors did this retirement
/// name", and the two answers differ exactly when a member names one successor, lets that hint leave, and names another
/// in the same term.
/// <para>
/// The evidence is recorded where the emission happens rather than derived from the messages when a state is built. Two
/// reasons: a state rebuilt with renamed members would derive a second copy of the same hint under the new labels, and
/// keeping every emission would make the fault unbounded. One record per member and term, turning from "one successor
/// named" to "a different one followed" once and never again, is both stable under renaming and finite.
/// </para>
/// </remarks>
internal static class ModelHintHistory
{
    /// <summary>Records that a member named one more successor in a term.</summary>
    /// <param name="previous">The evidence recorded so far.</param>
    /// <param name="sender">The member that named a successor.</param>
    /// <param name="term">The term it named the successor in.</param>
    /// <param name="successor">The successor it named.</param>
    /// <returns>
    /// The evidence including this naming, ordered by member then term; the same list when this naming tells it nothing
    /// new, which is the case for the first successor repeated and for every later naming once a second one is recorded.
    /// </returns>
    internal static IReadOnlyList<ModelHintEmission> With(
        IReadOnlyList<ModelHintEmission> previous,
        int sender,
        int term,
        int successor)
    {
        for (var i = 0; i < previous.Count; i++)
        {
            var known = previous[i];
            if (known.Sender != sender || known.Term != term)
                continue;

            if (known.SecondSuccessor >= 0 || known.FirstSuccessor == successor)
                return previous;

            List<ModelHintEmission> replaced = [.. previous];
            replaced[i] = known with { SecondSuccessor = successor };
            return replaced.AsReadOnly();
        }

        List<ModelHintEmission> added = [.. previous];
        added.Add(new ModelHintEmission(sender, term, successor, -1));
        added.Sort(static (a, b) => CompareOrder(in a, in b));
        return added.AsReadOnly();
    }

    /// <summary>Drops the evidence about members that can no longer name another successor in that term.</summary>
    /// <param name="previous">The evidence recorded so far.</param>
    /// <param name="nodes">The members of the state being built.</param>
    /// <returns>The evidence that can still change, in the same order; the same list when nothing is dropped.</returns>
    /// <remarks>
    /// A member names a successor only while it leads the term it names, so once it does not, its record can never gain a
    /// second successor: keeping it would split states that are one state as far as this policy is concerned, and under the
    /// sound rule — where the retirement demotes the leader in the same step — it would make every retirement carry a
    /// permanently distinguishing label. Dropping it costs no report, because the violation is raised at the step that
    /// records it, which is the step that took it.
    /// </remarks>
    internal static IReadOnlyList<ModelHintEmission> Prune(IReadOnlyList<ModelHintEmission> previous, IReadOnlyList<NodeState> nodes)
    {
        var drop = 0;
        for (var i = 0; i < previous.Count; i++)
        {
            if (!CanStillName(previous[i], nodes))
                drop++;
        }

        if (drop == 0)
            return previous;

        List<ModelHintEmission> kept = [];
        for (var i = 0; i < previous.Count; i++)
        {
            if (CanStillName(previous[i], nodes))
                kept.Add(previous[i]);
        }

        return kept.AsReadOnly();
    }

    private static bool CanStillName(in ModelHintEmission record, IReadOnlyList<NodeState> nodes)
    {
        var node = nodes[record.Sender];
        return node.Role == NodeRole.Leader && node.CurrentTerm == record.Term;
    }

    private static int CompareOrder(in ModelHintEmission a, in ModelHintEmission b)
    {
        var c = a.Sender.CompareTo(b.Sender);
        return c != 0 ? c : a.Term.CompareTo(b.Term);
    }
}
