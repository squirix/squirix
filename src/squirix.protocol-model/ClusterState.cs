using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
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

    internal string Fingerprint(bool symmetryReduce) => FingerprintHelper.Create(this, symmetryReduce);

    /// <summary>Returns the fingerprint of the state prefixed with the position in a commit trace.</summary>
    /// <param name="traceIndex">The number of trace points the state has already passed.</param>
    /// <returns>The trace index, a colon and the fingerprint without symmetry reduction.</returns>
    internal string TraceFingerprint(int traceIndex) => FingerprintHelper.CreateTrace(this, traceIndex);

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

        /// <summary>One builder per thread: building a fingerprint is synchronous and never re-enters itself, and the finished string is copied out.</summary>
        private static readonly ThreadLocal<StringBuilder> Builder = new(static () => new StringBuilder(InitialCapacity));

        internal static string Create(ClusterState state, bool symmetryReduce)
        {
            var sb = RentBuilder();
            AppendPrefix(sb, state.DiskLosses);
            AppendState(sb, state, symmetryReduce);
            return sb.ToString();
        }

        internal static string CreateTrace(ClusterState state, int traceIndex)
        {
            var sb = RentBuilder();
            _ = sb.Append(CultureInfo.InvariantCulture, $"{traceIndex}:");
            AppendPrefix(sb, state.DiskLosses);
            AppendState(sb, state, false);
            return sb.ToString();
        }

        private static void AppendCanonical(StringBuilder sb, ClusterState state)
        {
            var nodes = state.Nodes;
            var count = nodes.Count;
            Span<int> order = count <= MaxStackItems ? stackalloc int[count] : new int[count];
            Span<int> map = count <= MaxStackItems ? stackalloc int[count] : new int[count];
            BuildSymmetryOrder(nodes, order);
            for (var i = 0; i < count; i++)
                map[order[i]] = i;

            AppendNodePartitions(sb, state, order, map);
            AppendOrderedMessages(sb, state.Messages, map);
        }

        private static void AppendLog(StringBuilder sb, NodeState n)
        {
            for (var i = 0; i < n.LogEntries.Count; i++)
            {
                if (i > 0)
                    _ = sb.Append('/');

                var entry = n.LogEntries[i];
                _ = sb.Append(CultureInfo.InvariantCulture, $"{entry.Term}@{entry.Index}");
            }
        }

        private static void AppendMessage(StringBuilder sb, in InFlightMessage m, ReadOnlySpan<int> map) => _ = sb.Append(
            CultureInfo.InvariantCulture,
            $"{MsgKindOrdinal(m.Kind)},{Mapped(map, m.From)},{Mapped(map, m.To)},{m.Term},{m.LastLogIndex},{m.LastLogTerm},{(m.Success ? '1' : '0')},{m.MatchIndex},{m.ReadIndex},{m.PrevLogTerm}");

        private static void AppendNode(StringBuilder sb, NodeState n, in NodeIdentity identity)
        {
            _ = sb.Append(
                CultureInfo.InvariantCulture,
                $"{identity.Id},{RoleOrdinal(n.Role)},{n.CurrentTerm},{identity.VotedFor},{n.CommitIndex},{n.AppliedIndex},{identity.VotesGranted},{n.ReadIndex},{identity.ReadAcks},{(n.ReadReady ? '1' : '0')},{(n.Unseeded ? '1' : '0')},[");
            AppendLog(sb, n);
            _ = sb.Append(']');
        }

        private static void AppendNodePartitions(StringBuilder sb, ClusterState state, ReadOnlySpan<int> order, ReadOnlySpan<int> map)
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

                _ = sb.Append(CultureInfo.InvariantCulture, $"|{state.Partitions[source]}|{state.MatchIndexes[source]};");
            }
        }

        private static void AppendOrderedMessages(StringBuilder sb, IReadOnlyList<InFlightMessage> messages, ReadOnlySpan<int> map)
        {
            var count = messages.Count;
            Span<int> order = count <= MaxStackItems ? stackalloc int[count] : new int[count];
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
                _ = sb.Append(';');
            }
        }

        private static void AppendPrefix(StringBuilder sb, int diskLosses) => _ = sb.Append(CultureInfo.InvariantCulture, $"{diskLosses}#");

        private static void AppendState(StringBuilder sb, ClusterState state, bool symmetryReduce)
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
                var current = i;
                var j = i;
                while (j > 0 && comparer.Compare(order[j - 1], current) > 0)
                {
                    order[j] = order[j - 1];
                    j--;
                }

                order[j] = current;
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

        private static StringBuilder RentBuilder()
        {
            var sb = Builder.Value!;
            _ = sb.Clear();
            return sb;
        }

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

            private static int VotedRelation(NodeState n) => (n.VotedFor < 0, n.VotedFor == n.Id) switch
            {
                (true, _) => -1,
                (false, true) => 0,
                (false, false) => 1,
            };

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
        }
    }
}
