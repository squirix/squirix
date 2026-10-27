using System.Collections.Generic;

namespace Squirix.ProtocolModel;

internal static class ModelTransitionUtil
{
    internal static List<LogEntry> AppendEntry(IReadOnlyList<LogEntry> log, LogEntry entry)
    {
        var result = new List<LogEntry>(log.Count + 1);
        for (var i = 0; i < log.Count; i++)
        {
            if (log[i].Index == entry.Index)
                continue;

            if (log[i].Index > entry.Index)
                break;

            result.Add(log[i]);
        }

        result.Add(entry);
        return result;
    }

    internal static int[] CloneInts(IReadOnlyList<int> values)
    {
        var copy = new int[values.Count];
        for (var i = 0; i < values.Count; i++)
            copy[i] = values[i];

        return copy;
    }

    internal static List<InFlightMessage> CloneMessages(IReadOnlyList<InFlightMessage> messages)
    {
        var copy = new List<InFlightMessage>(messages.Count + 2);
        for (var i = 0; i < messages.Count; i++)
            copy.Add(messages[i]);

        return copy;
    }

    internal static NodeState[] CloneNodes(IReadOnlyList<NodeState> nodes)
    {
        var copy = new NodeState[nodes.Count];
        for (var i = 0; i < nodes.Count; i++)
            copy[i] = nodes[i];

        return copy;
    }

    /// <summary>Sends a leader's new last entry to every member it can reach.</summary>
    /// <param name="state">The state before the entry was appended.</param>
    /// <param name="from">The leader.</param>
    /// <param name="entry">The entry, of the leader's term.</param>
    /// <param name="prevLogTerm">The term of the entry before it in the leader's log, or 0 for the first entry.</param>
    /// <param name="messages">The messages in flight.</param>
    /// <param name="nextId">The next message id.</param>
    /// <returns>The next message id after the sends.</returns>
    internal static int EnqueueAppendEntries(ClusterState state, int from, LogEntry entry, int prevLogTerm, List<InFlightMessage> messages, int nextId)
    {
        for (var j = 0; j < state.Nodes.Count; j++)
        {
            if (j == from || !state.CanCommunicate(from, j))
                continue;

            messages.Add(new InFlightMessage(nextId++, MessagePayload.Append((from, j, entry.Term), entry, prevLogTerm)));
        }

        return nextId;
    }

    internal static int EnqueueReadIndexRequests(ClusterState state, int from, int term, int readIndex, List<InFlightMessage> messages, int nextId)
    {
        for (var j = 0; j < state.Nodes.Count; j++)
        {
            if (j == from || !state.CanCommunicate(from, j))
                continue;

            messages.Add(new InFlightMessage(nextId++, MessagePayload.ReadRequest(from, j, term, readIndex)));
        }

        return nextId;
    }

    internal static int EnqueueVoteRequests(ClusterState state, int from, int term, NodeState candidate, List<InFlightMessage> messages, int nextId)
    {
        for (var j = 0; j < state.Nodes.Count; j++)
        {
            if (j == from || !state.CanCommunicate(from, j))
                continue;

            messages.Add(new InFlightMessage(nextId++, MessagePayload.VoteRequest(from, j, term, candidate.LastLogIndex, candidate.LastLogTerm)));
        }

        return nextId;
    }

    internal static LogEntry? FindEntry(IReadOnlyList<LogEntry> log, int index)
    {
        for (var i = 0; i < log.Count; i++)
        {
            if (log[i].Index == index)
                return log[i];
        }

        return null;
    }

    internal static NodeState Patch(NodeState node, NodePatch patch)
    {
        return node.With(
            patch.Role ?? node.Role,
            patch.CurrentTerm ?? node.CurrentTerm,
            patch.VotedFor ?? node.VotedFor,
            patch.LogEntries ?? node.LogEntries,
            NodeRuntime.Create(
                patch.CommitIndex ?? node.CommitIndex,
                patch.AppliedIndex ?? node.AppliedIndex,
                patch.VotesGranted ?? node.VotesGranted,
                patch.ReadIndex ?? node.ReadIndex,
                patch.ReadAcks ?? node.ReadAcks,
                patch.ReadReady ?? node.ReadReady),
            patch.Unseeded ?? node.Unseeded);
    }

    internal static List<LogEntry> TruncateTo(IReadOnlyList<LogEntry> log, int lastIndex)
    {
        var result = new List<LogEntry>(log.Count);
        for (var i = 0; i < log.Count; i++)
        {
            if (log[i].Index <= lastIndex)
                result.Add(log[i]);
        }

        return result;
    }
}
