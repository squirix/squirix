using System.Collections.Generic;

namespace Squirix.ProtocolModel;

/// <summary>Retains commit evidence independently of the nodes that currently remember the commit index.</summary>
internal static class ModelCommitHistory
{
    internal static IReadOnlyList<ModelCommitDecision> Record(IReadOnlyList<ModelCommitDecision> previous, IReadOnlyList<NodeState> nodes)
    {
        List<ModelCommitDecision>? updated = null;
        for (var i = 0; i < nodes.Count; i++)
        {
            var node = nodes[i];
            for (var j = 0; j < node.LogEntries.Count; j++)
            {
                var entry = node.LogEntries[j];
                if (entry.Index <= node.CommitIndex)
                    RecordEntry(previous, ref updated, entry, node.CurrentTerm);
            }
        }

        if (updated == null)
            return previous;

        updated.Sort(static (a, b) => a.Index != b.Index ? a.Index.CompareTo(b.Index) : a.EntryTerm.CompareTo(b.EntryTerm));
        return updated.AsReadOnly();
    }

    private static void RecordEntry(IReadOnlyList<ModelCommitDecision> previous, ref List<ModelCommitDecision>? updated, LogEntry entry, int term)
    {
        IReadOnlyList<ModelCommitDecision> history = updated ?? previous;
        for (var i = 0; i < history.Count; i++)
        {
            var known = history[i];
            if (known.Index != entry.Index || known.EntryTerm != entry.Term)
                continue;
            if (known.CommitTerm > term)
            {
                updated ??= [.. previous];
                updated[i] = known with { CommitTerm = term };
            }

            return;
        }

        updated ??= [.. previous];
        updated.Add(new ModelCommitDecision(entry.Index, entry.Term, term));
    }
}
