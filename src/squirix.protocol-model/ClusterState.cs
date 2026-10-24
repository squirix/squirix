using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>Immutable Raft-equivalent cluster snapshot for exploration.</summary>
[Immutable]
internal sealed class ClusterState
{
    private ClusterState(
        IReadOnlyList<NodeState> nodes,
        IReadOnlyList<InFlightMessage> messages,
        IReadOnlyList<int> partitions,
        int nextMessageId,
        (IReadOnlyList<int> MatchIndexes, int DiskLosses) progress)
    {
        Nodes = CopyNodes(nodes);
        Messages = CopyMessages(messages);
        Partitions = CopyInts(partitions);
        NextMessageId = nextMessageId;
        MatchIndexes = CopyInts(progress.MatchIndexes);
        DiskLosses = progress.DiskLosses;
    }

    /// <summary>Gets the number of disk losses so far; a profile bounds it.</summary>
    internal int DiskLosses { get; }

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

        return new ClusterState(nodes, Array.Empty<InFlightMessage>(), partitions, 1, (matchIndexes, 0));
    }

    internal bool CanCommunicate(int from, int to) => Partitions[from] == Partitions[to];

    internal string Fingerprint(bool symmetryReduce) => FingerprintHelper.Write(this, symmetryReduce).ToString();

    /// <summary>Writes <see cref="Fingerprint"/> into a thread-local buffer and returns it, so a caller can look the state up before it allocates the string.</summary>
    /// <param name="symmetryReduce">Whether the fingerprint is reduced by node symmetry.</param>
    /// <returns>The fingerprint characters; valid until the next fingerprint is built on the same thread.</returns>
    internal ReadOnlySpan<char> FingerprintChars(bool symmetryReduce) => FingerprintHelper.Write(this, symmetryReduce).AsSpan();

    /// <summary>Returns the fingerprint of the state prefixed with the position in a commit trace.</summary>
    /// <param name="traceIndex">The number of trace points the state has already passed.</param>
    /// <returns>The trace index, a colon and the fingerprint without symmetry reduction.</returns>
    internal string TraceFingerprint(int traceIndex) => FingerprintHelper.WriteTrace(this, traceIndex).ToString();

    /// <summary>Writes <see cref="TraceFingerprint"/> into a thread-local buffer and returns it, so a caller can look the state up before it allocates the string.</summary>
    /// <param name="traceIndex">The number of trace points the state has already passed.</param>
    /// <returns>The trace fingerprint characters; valid until the next fingerprint is built on the same thread.</returns>
    internal ReadOnlySpan<char> TraceFingerprintChars(int traceIndex) => FingerprintHelper.WriteTrace(this, traceIndex).AsSpan();

    /// <summary>Returns the state after the disk of one node was lost.</summary>
    /// <param name="nodes">The nodes, the wiped one included.</param>
    /// <param name="matchIndexes">The match indexes, the wiped node's reset.</param>
    /// <returns>The state with one more disk loss.</returns>
    internal ClusterState WithDiskLoss(IReadOnlyList<NodeState> nodes, IReadOnlyList<int> matchIndexes) =>
        new(nodes, Messages, Partitions, NextMessageId, (matchIndexes, DiskLosses + 1));

    internal ClusterState WithMatchIndexes(IReadOnlyList<int> matchIndexes) => With(matchIndexes: matchIndexes);

    internal ClusterState WithMessages(IReadOnlyList<InFlightMessage> messages) => With(messages: messages);

    internal ClusterState WithMessages(IReadOnlyList<InFlightMessage> messages, int nextMessageId) => With(messages: messages, nextMessageId: nextMessageId);

    internal ClusterState WithNodes(IReadOnlyList<NodeState> nodes) => With(nodes);

    internal ClusterState WithNodesMatch(IReadOnlyList<NodeState> nodes, IReadOnlyList<int> matchIndexes) => With(nodes, matchIndexes: matchIndexes);

    internal ClusterState WithNodesMessages(IReadOnlyList<NodeState> nodes, IReadOnlyList<InFlightMessage> messages, int nextMessageId) =>
        With(nodes, messages, nextMessageId: nextMessageId);

    internal ClusterState WithNodesMessagesMatch(IReadOnlyList<NodeState> nodes, IReadOnlyList<InFlightMessage> messages, int nextMessageId, IReadOnlyList<int> matchIndexes) =>
        With(nodes, messages, nextMessageId: nextMessageId, matchIndexes: matchIndexes);

    internal ClusterState WithNodesMessagesMatch(IReadOnlyList<NodeState> nodes, IReadOnlyList<InFlightMessage> messages, IReadOnlyList<int> matchIndexes) =>
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
        IReadOnlyList<int>? matchIndexes = null) => new(
        nodes ?? Nodes,
        messages ?? Messages,
        partitions ?? Partitions,
        nextMessageId ?? NextMessageId,
        (matchIndexes ?? MatchIndexes, DiskLosses));

    private static class FingerprintHelper
    {
        private const int InitialCapacity = 512;
        private const int MaxStackItems = 64;

        /// <summary>One writer per thread: building a fingerprint is synchronous and never re-enters itself, and a finished string is copied out.</summary>
        private static readonly ThreadLocal<FingerprintWriter> Writer = new(static () => new FingerprintWriter(InitialCapacity));

        internal static FingerprintWriter Write(ClusterState state, bool symmetryReduce)
        {
            var sb = Writer.Value!;
            sb.Clear();
            AppendPrefix(sb, state.DiskLosses);
            AppendState(sb, state, symmetryReduce);
            return sb;
        }

        internal static FingerprintWriter WriteTrace(ClusterState state, int traceIndex)
        {
            var sb = Writer.Value!;
            sb.Clear();
            sb.Append(traceIndex);
            sb.Append(':');
            AppendPrefix(sb, state.DiskLosses);
            AppendState(sb, state, false);
            return sb;
        }

        private static void AppendCanonical(FingerprintWriter sb, ClusterState state)
        {
            var nodes = state.Nodes;
            var count = nodes.Count;
            var order = count <= MaxStackItems ? stackalloc int[count] : new int[count];
            var map = count <= MaxStackItems ? stackalloc int[count] : new int[count];
            BuildSymmetryOrder(nodes, order);
            for (var i = 0; i < count; i++)
                map[order[i]] = i;

            AppendNodePartitions(sb, state, order, map);
            AppendOrderedMessages(sb, state.Messages, map);
        }

        private static void AppendField(FingerprintWriter sb, int value)
        {
            sb.Append(value);
            sb.Append(',');
        }

        private static void AppendLog(FingerprintWriter sb, NodeState n)
        {
            for (var i = 0; i < n.LogEntries.Count; i++)
            {
                if (i > 0)
                    sb.Append('/');

                var entry = n.LogEntries[i];
                sb.Append(entry.Term);
                sb.Append('@');
                sb.Append(entry.Index);
            }
        }

        private static void AppendMessage(FingerprintWriter sb, in InFlightMessage m, ReadOnlySpan<int> map)
        {
            AppendField(sb, MsgKindOrdinal(m.Kind));
            AppendField(sb, Mapped(map, m.From));
            AppendField(sb, Mapped(map, m.To));
            AppendField(sb, m.Term);
            AppendField(sb, m.LastLogIndex);
            AppendField(sb, m.LastLogTerm);
            sb.AppendFlag(m.Success);
            sb.Append(',');
            AppendField(sb, m.MatchIndex);
            AppendField(sb, m.ReadIndex);
            sb.Append(m.PrevLogTerm);
        }

        private static void AppendNode(FingerprintWriter sb, NodeState n, in NodeIdentity identity)
        {
            AppendField(sb, identity.Id);
            AppendField(sb, RoleOrdinal(n.Role));
            AppendField(sb, n.CurrentTerm);
            AppendField(sb, identity.VotedFor);
            AppendField(sb, n.CommitIndex);
            AppendField(sb, n.AppliedIndex);
            AppendField(sb, identity.VotesGranted);
            AppendField(sb, n.ReadIndex);
            AppendField(sb, identity.ReadAcks);
            sb.AppendFlag(n.ReadReady);
            sb.Append(',');
            sb.AppendFlag(n.Unseeded);
            sb.Append(',');
            sb.Append('[');
            AppendLog(sb, n);
            sb.Append(']');
        }

        private static void AppendNodePartitions(FingerprintWriter sb, ClusterState state, ReadOnlySpan<int> order, ReadOnlySpan<int> map)
        {
            var nodes = state.Nodes;
            for (var i = 0; i < nodes.Count; i++)
            {
                var source = order.IsEmpty ? i : order[i];
                var n = nodes[source];
                if (map.IsEmpty)
                {
                    AppendNode(sb, n, new NodeIdentity(n.Id, n.VotedFor, n.VotesGranted, n.ReadAcks));
                }
                else
                {
                    var votedFor = n.VotedFor < 0 ? -1 : map[n.VotedFor];
                    AppendNode(sb, n, new NodeIdentity(i, votedFor, VoteMask.Remap(n.VotesGranted, map), VoteMask.Remap(n.ReadAcks, map)));
                }

                sb.Append('|');
                sb.Append(state.Partitions[source]);
                sb.Append('|');
                sb.Append(state.MatchIndexes[source]);
                sb.Append(';');
            }
        }

        private static void AppendOrderedMessages(FingerprintWriter sb, IReadOnlyList<InFlightMessage> messages, ReadOnlySpan<int> map)
        {
            var count = messages.Count;
            var order = count <= MaxStackItems ? stackalloc int[count] : new int[count];
            for (var i = 0; i < count; i++)
            {
                var current = messages[i];
                var j = i;
                while (j > 0)
                {
                    var previous = messages[order[j - 1]];
                    if (CompareMessages(in previous, in current, map) <= 0)
                        break;

                    order[j] = order[j - 1];
                    j--;
                }

                order[j] = i;
            }

            for (var i = 0; i < count; i++)
            {
                var m = messages[order[i]];
                AppendMessage(sb, in m, map);
                sb.Append(';');
            }
        }

        private static void AppendPrefix(FingerprintWriter sb, int diskLosses)
        {
            sb.Append(diskLosses);
            sb.Append('#');
        }

        private static void AppendState(FingerprintWriter sb, ClusterState state, bool symmetryReduce)
        {
            if (symmetryReduce)
            {
                AppendCanonical(sb, state);
                return;
            }

            AppendNodePartitions(sb, state, default, default);
            AppendOrderedMessages(sb, state.Messages, default);
        }

        private static void BuildSymmetryOrder(IReadOnlyList<NodeState> nodes, Span<int> order)
        {
            var comparer = new NodeOrderComparer(nodes);
            for (var i = 0; i < order.Length; i++)
            {
                var j = i;
                while (j > 0 && comparer.Compare(order[j - 1], i) > 0)
                {
                    order[j] = order[j - 1];
                    j--;
                }

                order[j] = i;
            }
        }

        private static int CompareMessageBodies(in InFlightMessage a, in InFlightMessage b)
        {
            var c = a.LastLogIndex.CompareTo(b.LastLogIndex);
            if (c != 0)
                return c;

            c = a.LastLogTerm.CompareTo(b.LastLogTerm);
            if (c != 0)
                return c;

            c = a.Success.CompareTo(b.Success);
            if (c != 0)
                return c;

            c = a.MatchIndex.CompareTo(b.MatchIndex);
            if (c != 0)
                return c;

            c = a.ReadIndex.CompareTo(b.ReadIndex);
            return c != 0 ? c : a.PrevLogTerm.CompareTo(b.PrevLogTerm);
        }

        private static int CompareMessageHeaders(in InFlightMessage a, in InFlightMessage b, ReadOnlySpan<int> map)
        {
            var c = MsgKindOrdinal(a.Kind).CompareTo(MsgKindOrdinal(b.Kind));
            if (c != 0)
                return c;

            c = Mapped(map, a.From).CompareTo(Mapped(map, b.From));
            if (c != 0)
                return c;

            c = Mapped(map, a.To).CompareTo(Mapped(map, b.To));
            return c != 0 ? c : a.Term.CompareTo(b.Term);
        }

        private static int CompareMessages(in InFlightMessage a, in InFlightMessage b, ReadOnlySpan<int> map)
        {
            var c = CompareMessageHeaders(in a, in b, map);
            return c != 0 ? c : CompareMessageBodies(in a, in b);
        }

        private static int Mapped(ReadOnlySpan<int> map, int id) => map.IsEmpty ? id : map[id];

        private static int MsgKindOrdinal(MsgKind kind) => kind switch
        {
            MsgKind.RequestVote => 0,
            MsgKind.VoteResponse => 1,
            MsgKind.AppendEntries => 2,
            MsgKind.AppendResponse => 3,
            MsgKind.ReadIndexRequest => 4,
            MsgKind.ReadIndexResponse => 5,
            _ => throw new ArgumentOutOfRangeException(nameof(kind), kind, "Unsupported enum value."),
        };

        private static int RoleOrdinal(NodeRole role) => role switch
        {
            NodeRole.Follower => 0,
            NodeRole.Candidate => 1,
            NodeRole.Leader => 2,
            _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unsupported enum value."),
        };

        [StructLayout(LayoutKind.Auto)]
        [Immutable]
        private readonly record struct NodeIdentity(int Id, int VotedFor, int VotesGranted, int ReadAcks);

        /// <summary>Orders nodes by their label-free fields; nodes equal in every field keep their original order.</summary>
        [Immutable]
        private readonly record struct NodeOrderComparer
        {
            private readonly IReadOnlyList<NodeState> _nodes;

            internal NodeOrderComparer(IReadOnlyList<NodeState> nodes)
            {
                _nodes = nodes;
            }

            public int Compare(int x, int y)
            {
                var a = _nodes[x];
                var b = _nodes[y];
                var c = CompareTermState(a, b);
                if (c == 0)
                    c = CompareQuorumState(a, b);

                if (c == 0)
                    c = CompareLogs(a.LogEntries, b.LogEntries);

                return c != 0 ? c : x.CompareTo(y);
            }

            private static int CompareLogs(IReadOnlyList<LogEntry> a, IReadOnlyList<LogEntry> b)
            {
                var shared = Math.Min(a.Count, b.Count);
                for (var i = 0; i < shared; i++)
                {
                    var c = a[i].Term.CompareTo(b[i].Term);
                    if (c == 0)
                        c = a[i].Index.CompareTo(b[i].Index);

                    if (c != 0)
                        return c;
                }

                return a.Count.CompareTo(b.Count);
            }

            private static int CompareQuorumState(NodeState a, NodeState b)
            {
                var c = VoteMask.CountGranted(a.VotesGranted).CompareTo(VoteMask.CountGranted(b.VotesGranted));
                if (c != 0)
                    return c;

                c = a.ReadIndex.CompareTo(b.ReadIndex);
                if (c != 0)
                    return c;

                c = VoteMask.CountGranted(a.ReadAcks).CompareTo(VoteMask.CountGranted(b.ReadAcks));
                if (c != 0)
                    return c;

                c = a.ReadReady.CompareTo(b.ReadReady);
                return c != 0 ? c : a.Unseeded.CompareTo(b.Unseeded);
            }

            private static int CompareTermState(NodeState a, NodeState b)
            {
                var c = RoleOrdinal(a.Role).CompareTo(RoleOrdinal(b.Role));
                if (c != 0)
                    return c;

                c = a.CurrentTerm.CompareTo(b.CurrentTerm);
                if (c != 0)
                    return c;

                c = VotedRelation(a).CompareTo(VotedRelation(b));
                if (c != 0)
                    return c;

                c = a.CommitIndex.CompareTo(b.CommitIndex);
                return c != 0 ? c : a.AppliedIndex.CompareTo(b.AppliedIndex);
            }

            private static int VotedRelation(NodeState n) => (n.VotedFor < 0, n.VotedFor == n.Id) switch
            {
                (true, _) => -1,
                (false, true) => 0,
                (false, false) => 1,
            };
        }
    }
}
