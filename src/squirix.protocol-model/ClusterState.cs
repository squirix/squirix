using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Squirix.ProtocolModel;

/// <summary>Immutable Raft-equivalent cluster snapshot for exploration.</summary>
internal sealed class ClusterState
{
    private ClusterState(
        IReadOnlyList<NodeState> nodes,
        IReadOnlyList<InFlightMessage> messages,
        IReadOnlyList<int> partitions,
        int nextMessageId,
        IReadOnlyList<int> matchIndexes)
    {
        Nodes = CopyNodes(nodes);
        Messages = CopyMessages(messages);
        Partitions = CopyInts(partitions);
        NextMessageId = nextMessageId;
        MatchIndexes = CopyInts(matchIndexes);
    }

    internal IReadOnlyList<int> MatchIndexes { get; }

    internal IReadOnlyList<InFlightMessage> Messages { get; }

    internal int NextMessageId { get; }

    internal IReadOnlyList<NodeState> Nodes { get; }

    internal IReadOnlyList<int> Partitions { get; }

    internal static ClusterState CreateInitial(int replicaCount)
    {
        var nodes = new NodeState[replicaCount];
        var partitions = new int[replicaCount];
        var matchIndexes = new int[replicaCount];
        for (var i = 0; i < replicaCount; i++)
        {
            nodes[i] = NodeState.CreateInitial(i);
            partitions[i] = 0;
            matchIndexes[i] = 0;
        }

        return new ClusterState(nodes, Array.Empty<InFlightMessage>(), partitions, 1, matchIndexes);
    }

    internal bool CanCommunicate(int from, int to) => Partitions[from] == Partitions[to];

    internal string Fingerprint(bool symmetryReduce) =>
        symmetryReduce ? FingerprintHelper.Canonical(this) : FingerprintHelper.Raw(this);

    internal ClusterState WithMatchIndexes(IReadOnlyList<int> matchIndexes) => With(matchIndexes: matchIndexes);

    internal ClusterState WithMessages(IReadOnlyList<InFlightMessage> messages) => With(messages: messages);

    internal ClusterState WithMessages(IReadOnlyList<InFlightMessage> messages, int nextMessageId) =>
        With(messages: messages, nextMessageId: nextMessageId);

    internal ClusterState WithNodes(IReadOnlyList<NodeState> nodes) => With(nodes);

    internal ClusterState WithNodesMatch(IReadOnlyList<NodeState> nodes, IReadOnlyList<int> matchIndexes) =>
        With(nodes, matchIndexes: matchIndexes);

    internal ClusterState WithNodesMessages(IReadOnlyList<NodeState> nodes, IReadOnlyList<InFlightMessage> messages, int nextMessageId) =>
        With(nodes, messages, nextMessageId: nextMessageId);

    internal ClusterState WithNodesMessagesMatch(
        IReadOnlyList<NodeState> nodes,
        IReadOnlyList<InFlightMessage> messages,
        int nextMessageId,
        IReadOnlyList<int> matchIndexes) =>
        With(nodes, messages, nextMessageId: nextMessageId, matchIndexes: matchIndexes);

    internal ClusterState WithNodesMessagesMatch(
        IReadOnlyList<NodeState> nodes,
        IReadOnlyList<InFlightMessage> messages,
        IReadOnlyList<int> matchIndexes) =>
        With(nodes, messages, matchIndexes: matchIndexes);

    internal ClusterState WithPartitions(IReadOnlyList<int> partitions) => With(partitions: partitions);

    private static int[] CopyInts(IReadOnlyList<int> values)
    {
        var copy = new int[values.Count];
        for (var i = 0; i < values.Count; i++)
            copy[i] = values[i];

        return copy;
    }

    private static InFlightMessage[] CopyMessages(IReadOnlyList<InFlightMessage> messages)
    {
        var copy = new InFlightMessage[messages.Count];
        for (var i = 0; i < messages.Count; i++)
            copy[i] = messages[i];

        return copy;
    }

    private static NodeState[] CopyNodes(IReadOnlyList<NodeState> nodes)
    {
        var copy = new NodeState[nodes.Count];
        for (var i = 0; i < nodes.Count; i++)
            copy[i] = nodes[i];

        return copy;
    }

    private ClusterState With(
        IReadOnlyList<NodeState>? nodes = null,
        IReadOnlyList<InFlightMessage>? messages = null,
        IReadOnlyList<int>? partitions = null,
        int? nextMessageId = null,
        IReadOnlyList<int>? matchIndexes = null) =>
        new(
            nodes ?? Nodes,
            messages ?? Messages,
            partitions ?? Partitions,
            nextMessageId ?? NextMessageId,
            matchIndexes ?? MatchIndexes);

    private static class FingerprintHelper
    {
        internal static string Canonical(ClusterState state) => Raw(RemapForSymmetry(state));

        internal static string Raw(ClusterState state)
        {
            var sb = new StringBuilder(256);
            AppendNodePartitions(sb, state);
            AppendOrderedMessages(sb, state);
            return sb.ToString();
        }

        private static void AppendLog(StringBuilder sb, NodeState n)
        {
            for (var i = 0; i < n.LogEntries.Count; i++)
            {
                if (i > 0)
                    _ = sb.Append('/');

                _ = sb.Append(n.LogEntries[i].Term.ToString(CultureInfo.InvariantCulture)).Append('@')
                      .Append(n.LogEntries[i].Index.ToString(CultureInfo.InvariantCulture));
            }
        }

        private static void AppendMessage(StringBuilder sb, InFlightMessage m)
        {
            _ = sb.Append(MsgKindOrdinal(m.Kind).ToString(CultureInfo.InvariantCulture)).Append(',').Append(m.From.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(m.To.ToString(CultureInfo.InvariantCulture)).Append(',').Append(m.Term.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(m.LastLogIndex.ToString(CultureInfo.InvariantCulture)).Append(',').Append(m.LastLogTerm.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(m.Success ? '1' : '0').Append(',').Append(m.MatchIndex.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(m.ReadIndex.ToString(CultureInfo.InvariantCulture));
        }

        private static void AppendNode(StringBuilder sb, NodeState n)
        {
            _ = sb.Append(n.Id.ToString(CultureInfo.InvariantCulture)).Append(',').Append(RoleOrdinal(n.Role).ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(n.CurrentTerm.ToString(CultureInfo.InvariantCulture)).Append(',').Append(n.VotedFor.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(n.CommitIndex.ToString(CultureInfo.InvariantCulture)).Append(',').Append(n.AppliedIndex.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(n.VotesGranted.ToString(CultureInfo.InvariantCulture)).Append(',').Append(n.ReadIndex.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(n.ReadAcks.ToString(CultureInfo.InvariantCulture)).Append(',').Append(n.ReadReady ? '1' : '0').Append(',').Append(n.BadOldCommit ? '1' : '0')
                  .Append(",[");
            AppendLog(sb, n);
            _ = sb.Append(']');
        }

        private static void AppendNodePartitions(StringBuilder sb, ClusterState state)
        {
            for (var i = 0; i < state.Nodes.Count; i++)
            {
                AppendNode(sb, state.Nodes[i]);
                _ = sb.Append('|').Append(state.Partitions[i].ToString(CultureInfo.InvariantCulture)).Append('|')
                      .Append(state.MatchIndexes[i].ToString(CultureInfo.InvariantCulture)).Append(';');
            }
        }

        private static void AppendNodeStructure(StringBuilder sb, NodeState n)
        {
            int votedRel;
            if (n.VotedFor < 0)
                votedRel = -1;
            else if (n.VotedFor == n.Id)
                votedRel = 0;
            else
                votedRel = 1;

            _ = sb.Append(RoleOrdinal(n.Role).ToString(CultureInfo.InvariantCulture)).Append(',').Append(n.CurrentTerm.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(votedRel.ToString(CultureInfo.InvariantCulture)).Append(',').Append(n.CommitIndex.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(n.AppliedIndex.ToString(CultureInfo.InvariantCulture)).Append(',').Append(VoteMask.CountGranted(n.VotesGranted).ToString(CultureInfo.InvariantCulture))
                  .Append(',').Append(n.ReadIndex.ToString(CultureInfo.InvariantCulture)).Append(',')
                  .Append(VoteMask.CountGranted(n.ReadAcks).ToString(CultureInfo.InvariantCulture)).Append(',').Append(n.ReadReady ? '1' : '0').Append(',')
                  .Append(n.BadOldCommit ? '1' : '0').Append(",[");
            AppendLog(sb, n);
            _ = sb.Append(']');
        }

        private static void AppendOrderedMessages(StringBuilder sb, ClusterState state)
        {
            var ordered = new List<InFlightMessage>(state.Messages.Count);
            for (var i = 0; i < state.Messages.Count; i++)
                ordered.Add(state.Messages[i]);

            ordered.Sort(static (a, b) => CompareMessages(a, b));
            for (var i = 0; i < ordered.Count; i++)
            {
                AppendMessage(sb, ordered[i]);
                _ = sb.Append(';');
            }
        }

        private static int[] BuildSymmetryOrder(ClusterState state)
        {
            var order = new int[state.Nodes.Count];
            for (var i = 0; i < order.Length; i++)
                order[i] = i;

            Array.Sort(
                order,
                (a, b) =>
                {
                    var cmp = NodeSignature(state.Nodes[a]).CompareTo(NodeSignature(state.Nodes[b]), StringComparison.Ordinal);
                    return cmp is not 0 ? cmp : a.CompareTo(b);
                });
            return order;
        }

        private static int CompareMessages(InFlightMessage a, InFlightMessage b)
        {
            var c = a.Kind.CompareTo(b.Kind);
            if (c is not 0)
                return c;

            c = a.From.CompareTo(b.From);
            if (c is not 0)
                return c;

            c = a.To.CompareTo(b.To);
            if (c is not 0)
                return c;

            c = a.Term.CompareTo(b.Term);
            if (c is not 0)
                return c;

            c = a.LastLogIndex.CompareTo(b.LastLogIndex);
            if (c is not 0)
                return c;

            c = a.LastLogTerm.CompareTo(b.LastLogTerm);
            if (c is not 0)
                return c;

            c = a.Success.CompareTo(b.Success);
            if (c is not 0)
                return c;

            c = a.MatchIndex.CompareTo(b.MatchIndex);
            if (c is not 0)
                return c;

            return a.ReadIndex.CompareTo(b.ReadIndex);
        }

        private static int MsgKindOrdinal(MsgKind kind) =>
            kind switch
            {
                MsgKind.RequestVote => 0,
                MsgKind.VoteResponse => 1,
                MsgKind.AppendEntries => 2,
                MsgKind.AppendResponse => 3,
                MsgKind.ReadIndexRequest => 4,
                MsgKind.ReadIndexResponse => 5,
                _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported enum value."),
            };

        private static string NodeSignature(NodeState n)
        {
            var sb = new StringBuilder(64);
            AppendNodeStructure(sb, n);
            return sb.ToString();
        }

        private static ClusterState RemapForSymmetry(ClusterState state)
        {
            var order = BuildSymmetryOrder(state);
            var map = new int[state.Nodes.Count];
            for (var i = 0; i < order.Length; i++)
                map[order[i]] = i;

            var remapped = RemapNodes(state, order, map);
            var parts = RemapInts(order, state.Partitions);
            var matches = RemapInts(order, state.MatchIndexes);
            var msgs = RemapMessages(state, map);
            return state.With(remapped, msgs, parts, matchIndexes: matches);
        }

        private static int[] RemapInts(int[] order, IReadOnlyList<int> source)
        {
            var result = new int[order.Length];
            for (var i = 0; i < order.Length; i++)
                result[i] = source[order[i]];

            return result;
        }

        private static InFlightMessage[] RemapMessages(ClusterState state, int[] map)
        {
            var messages = new InFlightMessage[state.Messages.Count];
            for (var i = 0; i < state.Messages.Count; i++)
            {
                var m = state.Messages[i];
                messages[i] = new InFlightMessage(
                    m.Id,
                    new MessagePayload(
                        new MessageRoute(m.Kind, map[m.From], map[m.To], m.Term),
                        new MessageExtras(m.LastLogIndex, m.LastLogTerm, m.Success, m.MatchIndex, m.ReadIndex)));
            }

            return messages;
        }

        private static NodeState[] RemapNodes(ClusterState state, int[] order, int[] map)
        {
            var remapped = new NodeState[state.Nodes.Count];
            for (var i = 0; i < order.Length; i++)
            {
                var src = state.Nodes[order[i]];
                remapped[i] = new NodeState(
                    i,
                    src.Role,
                    src.CurrentTerm,
                    src.VotedFor < 0 ? -1 : map[src.VotedFor],
                    src.LogEntries,
                    NodeRuntime.Create(
                        src.CommitIndex,
                        src.AppliedIndex,
                        VoteMask.Remap(src.VotesGranted, map),
                        src.ReadIndex,
                        VoteMask.Remap(src.ReadAcks, map),
                        src.ReadReady,
                        src.BadOldCommit));
            }

            return remapped;
        }

        private static int RoleOrdinal(NodeRole role) =>
            role switch
            {
                NodeRole.Follower => 0,
                NodeRole.Candidate => 1,
                NodeRole.Leader => 2,
                _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unsupported enum value."),
            };
    }
}
