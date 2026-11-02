using System;

namespace Squirix.ProtocolModel.Tests;

internal static class ClientHistoryQueries
{
    internal static bool Contains(NodeState node, LogEntry entry)
    {
        for (var i = 0; i < node.LogEntries.Count; i++)
        {
            if (node.LogEntries[i] == entry)
                return true;
        }

        return false;
    }

    internal static InFlightMessage Message(ClusterState state, MsgKind kind, int from, int to)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message.Kind == kind && message.From == from && message.To == to)
                return message;
        }

        throw new InvalidOperationException("Expected message is absent");
    }

    internal static bool HasMessage(ClusterState state, int id)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (state.Messages[i].Id == id)
                return true;
        }

        return false;
    }

    internal static bool HasAppend(ClusterState state, int from, int to, int index)
    {
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message.Kind == MsgKind.AppendEntries && message.From == from && message.To == to && message.LastLogIndex == index)
                return true;
        }

        return false;
    }

    internal static bool SameNodes(ClusterState before, ClusterState after)
    {
        for (var i = 0; i < before.Nodes.Count; i++)
        {
            if (!ReferenceEquals(before.Nodes[i], after.Nodes[i]))
                return false;
        }

        return true;
    }
}
