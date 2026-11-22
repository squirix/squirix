using System.Collections.Generic;

namespace Squirix.ProtocolModel;

/// <summary>Retains election evidence independently of the roles the nodes currently hold.</summary>
/// <remarks>
/// A leader that retires becomes a follower and keeps its term, so a checker that reads the current roles loses the fact
/// that it had been elected. Two distinct nodes elected in the same term are then invisible as soon as the first one
/// leaves the role, which is the gap this record closes: the evidence is kept as history and never removed.
/// </remarks>
internal static class ModelElectionHistory
{
    /// <summary>Adds the nodes that currently lead, keeping what earlier states already recorded.</summary>
    /// <param name="previous">The evidence recorded so far.</param>
    /// <param name="nodes">The nodes of the state being built.</param>
    /// <returns>The evidence including this state's leaders, ordered by term then node; the same list when nothing is new.</returns>
    internal static IReadOnlyList<ModelElectionRecord> Record(IReadOnlyList<ModelElectionRecord> previous, IReadOnlyList<NodeState> nodes)
    {
        List<ModelElectionRecord>? updated = null;
        for (var i = 0; i < nodes.Count; i++)
        {
            var node = nodes[i];
            if (node.Role != NodeRole.Leader || Contains(previous, node.CurrentTerm, node.Id))
                continue;

            updated ??= [.. previous];
            updated.Add(new ModelElectionRecord(node.CurrentTerm, node.Id));
        }

        if (updated == null)
            return previous;

        updated.Sort(static (a, b) => a.Term != b.Term ? a.Term.CompareTo(b.Term) : a.NodeId.CompareTo(b.NodeId));
        return updated.AsReadOnly();
    }

    private static bool Contains(IReadOnlyList<ModelElectionRecord> history, int term, int nodeId)
    {
        for (var i = 0; i < history.Count; i++)
        {
            if (history[i].Term == term && history[i].NodeId == nodeId)
                return true;
        }

        return false;
    }
}
