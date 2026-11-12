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
        (IReadOnlyList<int> MatchIndexes, int DiskLosses, IReadOnlyList<ModelCommitDecision> Commits, IReadOnlyList<ModelElectionRecord> Elections, IReadOnlyList<ModelHintEmission> Hints) progress,
        ClusterState? previous = null)
    {
        // Only collections already owned by the previous immutable state can be shared.
        Nodes = previous != null && HasSameItems(nodes, previous.Nodes) ? previous.Nodes : CopyItems(nodes);
        Messages = previous != null && ReferenceEquals(messages, previous.Messages) ? messages : CopyItems(messages);
        Partitions = previous != null && HasSameItems(partitions, previous.Partitions) ? previous.Partitions : CopyItems(partitions);
        NextMessageId = nextMessageId;
        MatchIndexes = previous != null && HasSameItems(progress.MatchIndexes, previous.MatchIndexes) ? previous.MatchIndexes : CopyItems(progress.MatchIndexes);
        DiskLosses = progress.DiskLosses;
        CommittedEntries = previous != null && ReferenceEquals(Nodes, previous.Nodes)
            ? previous.CommittedEntries
            : ModelCommitHistory.Record(progress.Commits, Nodes);
        ElectedLeaders = previous != null && ReferenceEquals(Nodes, previous.Nodes) && ReferenceEquals(progress.Elections, previous.ElectedLeaders)
            ? previous.ElectedLeaders
            : ModelElectionHistory.Record(progress.Elections, Nodes);
        HintEmissions = ModelHintHistory.Prune(progress.Hints, Nodes);
    }

    private ClusterState(ClusterState previous, int removedMessageIndex, IReadOnlyList<NodeState>? nodes, IReadOnlyList<int>? matchIndexes)
    {
        var count = previous.Messages.Count - 1;
        var messages = count == 0 ? [] : new InFlightMessage[count];
        for (var i = 0; i < count; i++)
            messages[i] = previous.Messages[i < removedMessageIndex ? i : i + 1];

        Nodes = nodes == null || HasSameItems(nodes, previous.Nodes) ? previous.Nodes : CopyItems(nodes);
        Messages = messages;
        Partitions = previous.Partitions;
        NextMessageId = previous.NextMessageId;
        MatchIndexes = matchIndexes == null || HasSameItems(matchIndexes, previous.MatchIndexes) ? previous.MatchIndexes : CopyItems(matchIndexes);
        DiskLosses = previous.DiskLosses;
        CommittedEntries = ReferenceEquals(Nodes, previous.Nodes)
            ? previous.CommittedEntries
            : ModelCommitHistory.Record(previous.CommittedEntries, Nodes);
        ElectedLeaders = ReferenceEquals(Nodes, previous.Nodes)
            ? previous.ElectedLeaders
            : ModelElectionHistory.Record(previous.ElectedLeaders, Nodes);
        HintEmissions = ModelHintHistory.Prune(previous.HintEmissions, Nodes);
    }

    /// <summary>Gets persistent checker evidence, including the decision term rather than only the entry term.</summary>
    internal IReadOnlyList<ModelCommitDecision> CommittedEntries { get; }

    /// <summary>Gets the number of disk losses so far; a profile bounds it.</summary>
    internal int DiskLosses { get; }

    /// <summary>
    /// Gets the election evidence, one record per node that has held leadership in the term it held it in.
    /// A leader that retires stops being a leader, so this is what keeps the election visible to the checker afterwards.
    /// </summary>
    internal IReadOnlyList<ModelElectionRecord> ElectedLeaders { get; }

    /// <summary>
    /// Gets the successor-hint evidence, one record per member and term: the first successor it named there and, once, the
    /// first different one. A hint is delivered or dropped before the next step, so this is what keeps the naming visible
    /// afterwards, and it is bounded so that a leader which keeps emitting does not make the state space infinite.
    /// </summary>
    internal IReadOnlyList<ModelHintEmission> HintEmissions { get; }

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

        return new ClusterState(nodes, Array.Empty<InFlightMessage>(), partitions, 1, (matchIndexes, 0, Array.Empty<ModelCommitDecision>(), Array.Empty<ModelElectionRecord>(), Array.Empty<ModelHintEmission>()));
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

    /// <summary>Returns the state with the named members replaced, the rest carried over.</summary>
    /// <param name="nodes">The nodes, or <see langword="null" /> to keep them.</param>
    /// <param name="messages">The messages, or <see langword="null" /> to keep them.</param>
    /// <param name="partitions">The partitions, or <see langword="null" /> to keep them.</param>
    /// <param name="nextMessageId">The next message id, or <see langword="null" /> to keep it.</param>
    /// <param name="matchIndexes">The match indexes, or <see langword="null" /> to keep them.</param>
    /// <param name="elections">The election evidence, or <see langword="null" /> to keep it.</param>
    /// <param name="hints">The hint evidence, or <see langword="null" /> to keep it.</param>
    /// <returns>The state.</returns>
    internal ClusterState With(
        IReadOnlyList<NodeState>? nodes = null,
        IReadOnlyList<InFlightMessage>? messages = null,
        IReadOnlyList<int>? partitions = null,
        int? nextMessageId = null,
        IReadOnlyList<int>? matchIndexes = null,
        IReadOnlyList<ModelElectionRecord>? elections = null,
        IReadOnlyList<ModelHintEmission>? hints = null) => new(
        nodes ?? Nodes,
        messages ?? Messages,
        partitions ?? Partitions,
        nextMessageId ?? NextMessageId,
        (matchIndexes ?? MatchIndexes, DiskLosses, CommittedEntries, elections ?? ElectedLeaders, hints ?? HintEmissions),
        this);

    /// <summary>Returns the state after the disk of one node was lost.</summary>
    /// <param name="nodes">The nodes, the wiped one included.</param>
    /// <param name="matchIndexes">The match indexes, the wiped node's reset.</param>
    /// <returns>The state with one more disk loss.</returns>
    internal ClusterState WithDiskLoss(IReadOnlyList<NodeState> nodes, IReadOnlyList<int> matchIndexes) =>
        new(nodes, Messages, Partitions, NextMessageId, (matchIndexes, DiskLosses + 1, CommittedEntries, ElectedLeaders, HintEmissions), this);

    internal ClusterState WithMatchIndexes(IReadOnlyList<int> matchIndexes) => With(matchIndexes: matchIndexes);

    internal ClusterState WithMessages(IReadOnlyList<InFlightMessage> messages) => With(messages: messages);

    /// <summary>Returns the state with the messages replaced.</summary>
    /// <param name="messages">The messages.</param>
    /// <param name="nextMessageId">The next message id.</param>
    /// <param name="hints">The hint evidence after this step, or <see langword="null" /> to keep it.</param>
    /// <returns>The state.</returns>
    internal ClusterState WithMessages(IReadOnlyList<InFlightMessage> messages, int nextMessageId, IReadOnlyList<ModelHintEmission>? hints = null) =>
        new(Nodes, messages, Partitions, nextMessageId, (MatchIndexes, DiskLosses, CommittedEntries, ElectedLeaders, hints ?? HintEmissions), this);

    internal ClusterState WithNodes(IReadOnlyList<NodeState> nodes) => With(nodes);

    internal ClusterState WithNodesMatch(IReadOnlyList<NodeState> nodes, IReadOnlyList<int> matchIndexes) => With(nodes, matchIndexes: matchIndexes);

    /// <summary>Returns the state with members and messages replaced.</summary>
    /// <param name="nodes">The nodes.</param>
    /// <param name="messages">The messages.</param>
    /// <param name="nextMessageId">The next message id.</param>
    /// <param name="hints">The hint evidence after this step, or <see langword="null" /> to keep it.</param>
    /// <returns>The state.</returns>
    internal ClusterState WithNodesMessages(IReadOnlyList<NodeState> nodes, IReadOnlyList<InFlightMessage> messages, int nextMessageId, IReadOnlyList<ModelHintEmission>? hints = null) =>
        new(nodes, messages, Partitions, nextMessageId, (MatchIndexes, DiskLosses, CommittedEntries, ElectedLeaders, hints ?? HintEmissions), this);

    internal ClusterState WithNodesMessagesMatch(IReadOnlyList<NodeState> nodes, IReadOnlyList<InFlightMessage> messages, int nextMessageId, IReadOnlyList<int> matchIndexes) =>
        With(nodes, messages, Partitions, nextMessageId, matchIndexes);

    internal ClusterState WithNodesMessagesMatch(IReadOnlyList<NodeState> nodes, IReadOnlyList<InFlightMessage> messages, IReadOnlyList<int> matchIndexes) =>
        With(nodes, messages, matchIndexes: matchIndexes);

    internal ClusterState WithPartitions(IReadOnlyList<int> partitions) => With(partitions: partitions);

    /// <summary>Returns a state with one message removed, keeping the order and identity of the others.</summary>
    /// <param name="messageIndex">The position of the message to remove.</param>
    /// <param name="nodes">The replacement nodes, or null to keep them.</param>
    /// <param name="matchIndexes">The replacement match indexes, or null to keep them.</param>
    /// <returns>The state after the message was removed.</returns>
    internal ClusterState WithoutMessage(int messageIndex, IReadOnlyList<NodeState>? nodes = null, IReadOnlyList<int>? matchIndexes = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(messageIndex);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(messageIndex, Messages.Count);
        return new ClusterState(this, messageIndex, nodes, matchIndexes);
    }

    private static T[] CopyItems<T>(IReadOnlyList<T> values)
    {
        var copy = new T[values.Count];
        for (var i = 0; i < values.Count; i++)
            copy[i] = values[i];

        return copy;
    }

    private static bool HasSameItems<T>(IReadOnlyList<T> values, IReadOnlyList<T> previous)
    {
        if (ReferenceEquals(values, previous))
            return true;

        if (values.Count != previous.Count)
            return false;

        for (var i = 0; i < values.Count; i++)
        {
            if (!EqualityComparer<T>.Default.Equals(values[i], previous[i]))
                return false;
        }

        return true;
    }

    private static class FingerprintHelper
    {
        private const int InitialCapacity = 512;
        private const int MaxStackItems = 64;

        /// <summary>One writer per thread: building a fingerprint is synchronous and never re-enters itself, and a finished string is copied out.</summary>
        private static readonly ThreadLocal<FingerprintWriter> Writer = new(static () => new FingerprintWriter(InitialCapacity));

        /// <summary>A second buffer, so a candidate labelling can be built while the chosen one is being compared with it.</summary>
        private static readonly ThreadLocal<FingerprintWriter> Candidate = new(static () => new FingerprintWriter(InitialCapacity));

        private static readonly ThreadLocal<FingerprintWriter> BestCandidate = new(static () => new FingerprintWriter(InitialCapacity));

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

        /// <summary>Writes the state with its members relabelled by symmetry order.</summary>
        /// <param name="sb">The writer to append to.</param>
        /// <param name="state">The state to write.</param>
        private static void AppendCanonical(FingerprintWriter sb, ClusterState state) => CanonicalLabelling.Append(sb, state);

        /// <summary>Appends the retained election evidence, relabelled by the same map as the nodes it names.</summary>
        /// <param name="sb">The writer to append to.</param>
        /// <param name="elections">The election evidence retained so far.</param>
        /// <param name="map">The relabelling, empty when the state keeps its own labels.</param>
        /// <remarks>
        /// The evidence names a node, so it is a member the symmetry key has to relabel like any other. Written with the ids
        /// the state happens to hold, two states that differ only by renaming keep different keys, the exploration stops
        /// merging them, and the reduction silently costs the saving it exists to give.
        /// </remarks>
        private static void AppendElectionHistory(FingerprintWriter sb, IReadOnlyList<ModelElectionRecord> elections, ReadOnlySpan<int> map)
        {
            var count = elections.Count;
            var order = count <= MaxStackItems ? stackalloc int[count] : new int[count];
            for (var i = 0; i < count; i++)
            {
                var current = elections[i];
                var j = i;
                while (j > 0)
                {
                    var previous = elections[order[j - 1]];
                    if (CompareElections(in previous, in current, map) <= 0)
                        break;

                    order[j] = order[j - 1];
                    j--;
                }

                order[j] = i;
            }

            sb.Append('E');
            for (var i = 0; i < count; i++)
            {
                var election = elections[order[i]];
                sb.Append(election.Term);
                sb.Append('@');
                sb.Append(Mapped(map, election.NodeId));
                sb.Append(';');
            }
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
            AppendField(sb, n.DurableCommitIndex);
            AppendField(sb, n.AppliedIndex);
            AppendField(sb, identity.VotesGranted);
            AppendField(sb, n.ReadIndex);
            AppendField(sb, identity.ReadAcks);
            sb.AppendFlag(n.ReadReady);
            sb.Append(',');
            sb.AppendFlag(n.Stopping);
            sb.Append(',');
            sb.AppendFlag(n.Unseeded);
            sb.Append(',');
            AppendField(sb, n.KnownLeaderTerm);
            AppendField(sb, identity.KnownLeaderId);
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
                    AppendNode(sb, n, new NodeIdentity(n.Id, n.VotedFor, n.VotesGranted, n.ReadAcks, n.KnownLeaderId));
                }
                else
                {
                    var votedFor = n.VotedFor < 0 ? -1 : map[n.VotedFor];
                    var knownLeader = n.KnownLeaderId < 0 ? -1 : map[n.KnownLeaderId];
                    AppendNode(
                        sb,
                        n,
                        new NodeIdentity(i, votedFor, VoteMask.Remap(n.VotesGranted, map), VoteMask.Remap(n.ReadAcks, map), knownLeader));
                }

                sb.Append('|');
                sb.Append(state.Partitions[source]);
                sb.Append('|');
                sb.Append(state.MatchIndexes[source]);
                sb.Append(';');
            }

            if (state.ElectedLeaders.Count > 0)
                AppendElectionHistory(sb, state.ElectedLeaders, map);

            if (state.HintEmissions.Count > 0)
                HintHistoryKey.Append(sb, state.HintEmissions, map);
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
            if (state.CommittedEntries.Count > 0)
            {
                sb.Append('C');
                for (var i = 0; i < state.CommittedEntries.Count; i++)
                {
                    var commit = state.CommittedEntries[i];
                    sb.Append(commit.Index);
                    sb.Append('@');
                    sb.Append(commit.EntryTerm);
                    sb.Append(':');
                    sb.Append(commit.CommitTerm);
                    sb.Append(';');
                }

                sb.Append('#');
            }

            if (symmetryReduce)
            {
                AppendCanonical(sb, state);
                return;
            }

            AppendNodePartitions(sb, state, default, default);
            AppendOrderedMessages(sb, state.Messages, default);
        }

        private static int CompareElections(in ModelElectionRecord a, in ModelElectionRecord b, ReadOnlySpan<int> map)
        {
            var c = a.Term.CompareTo(b.Term);
            return c != 0 ? c : Mapped(map, a.NodeId).CompareTo(Mapped(map, b.NodeId));
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
            MsgKind.TransferHint => 6,
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
        private readonly record struct NodeIdentity(int Id, int VotedFor, int VotesGranted, int ReadAcks, int KnownLeaderId);

        /// <summary>Orders nodes by their label-free fields; nodes equal in every field keep their original order.</summary>
        [Immutable]
        private readonly record struct NodeOrderComparer
        {
            private readonly IReadOnlyList<NodeState> _nodes;

            internal NodeOrderComparer(IReadOnlyList<NodeState> nodes)
            {
                _nodes = nodes;
            }

            internal int Compare(int x, int y)
            {
                var c = CompareKey(x, y);
                return c != 0 ? c : x.CompareTo(y);
            }

            /// <summary>Compares the two members on the fields the key prints, and reports a tie when it finds none.</summary>
            /// <param name="x">The first member.</param>
            /// <param name="y">The second member.</param>
            /// <returns>The comparison, without the label tie-break <see cref="Compare" /> adds.</returns>
            internal int CompareKey(int x, int y)
            {
                var a = _nodes[x];
                var b = _nodes[y];
                var c = CompareTermState(a, b);
                if (c == 0)
                    c = CompareQuorumState(a, b);

                if (c == 0)
                    c = CompareLogs(a.LogEntries, b.LogEntries);

                return c;
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
                if (c != 0)
                    return c;

                c = a.Stopping.CompareTo(b.Stopping);
                if (c != 0)
                    return c;

                c = a.KnownLeaderTerm.CompareTo(b.KnownLeaderTerm);
                if (c != 0)
                    return c;

                c = KnownLeaderRelation(a).CompareTo(KnownLeaderRelation(b));
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
                if (c != 0)
                    return c;
                c = a.DurableCommitIndex.CompareTo(b.DurableCommitIndex);
                return c != 0 ? c : a.AppliedIndex.CompareTo(b.AppliedIndex);
            }

            /// <summary>Classifies a node's known leader against its own label, so the ordering never reads an id.</summary>
            /// <param name="node">The node.</param>
            /// <returns>-1 for no known leader, 0 for itself, 1 for another member.</returns>
            private static int KnownLeaderRelation(NodeState node) => (node.KnownLeaderId < 0, node.KnownLeaderId == node.Id) switch
            {
                (true, _) => -1,
                (false, true) => 0,
                (false, false) => 1,
            };

            private static int VotedRelation(NodeState n) => (n.VotedFor < 0, n.VotedFor == n.Id) switch
            {
                (true, _) => -1,
                (false, true) => 0,
                (false, false) => 1,
            };
        }

        /// <summary>Relabels the members so the key depends on the state and not on the labels.</summary>
        /// <remarks>
        /// The order leaves two members tied when every field the key compares is equal, and ordering tied members by id makes
        /// the key depend on the labels it exists to forget: two states that differ only by renaming then print their votes and
        /// messages against different ranks and get different keys, so the exploration stops merging them and a comparison
        /// between the reduced and unreduced runs measures the tie-break rather than the model. The labellings of each tied run
        /// are enumerated and the smallest is kept, because renaming permutes that candidate set and the smallest element of a
        /// permuted set is the same element.
        /// </remarks>
        private static class CanonicalLabelling
        {
            private const int MaxLabellings = 24;

            /// <summary>Appends the smallest labelling of the state under the members the ordering key cannot separate.</summary>
            /// <param name="sb">The writer to append to.</param>
            /// <param name="state">The state to write.</param>
            internal static void Append(FingerprintWriter sb, ClusterState state)
            {
                var nodes = state.Nodes;
                var count = nodes.Count;
                var order = count <= MaxStackItems ? stackalloc int[count] : new int[count];
                BuildSymmetryOrder(nodes, order);

                var starts = count <= MaxStackItems ? stackalloc int[count] : new int[count];
                var lengths = count <= MaxStackItems ? stackalloc int[count] : new int[count];
                var runCount = BuildTieRuns(nodes, order, starts, lengths);
                if (runCount == 0)
                {
                    AppendBody(sb, state, order);
                    return;
                }

                var labellings = 1;
                for (var i = 0; i < runCount; i++)
                    labellings *= Factorial(lengths[i]);

                if (labellings > MaxLabellings)
                {
                    // Too many tied members to enumerate: this state keeps its own labels and is never merged with another.
                    // Nothing becomes unreachable; the run only loses the saving the reduction would have given it.
                    sb.Append('!');
                    AppendNodePartitions(sb, state, default, default);
                    AppendOrderedMessages(sb, state.Messages, default);
                    return;
                }

                AppendSmallest(sb, state, order, starts[..runCount], lengths[..runCount], labellings);
            }

            private static void AppendBody(FingerprintWriter sb, ClusterState state, ReadOnlySpan<int> order)
            {
                var map = order.Length <= MaxStackItems ? stackalloc int[order.Length] : new int[order.Length];
                ApplyOrder(order, map);
                AppendNodePartitions(sb, state, order, map);
                AppendOrderedMessages(sb, state.Messages, map);
            }

            private static void AppendSmallest(FingerprintWriter sb, ClusterState state, ReadOnlySpan<int> order, ReadOnlySpan<int> starts, ReadOnlySpan<int> lengths, int labellings)
            {
                var candidate = Candidate.Value!;
                var best = BestCandidate.Value!;
                best.Clear();
                var listed = order.Length <= MaxStackItems ? stackalloc int[order.Length] : new int[order.Length];
                var map = order.Length <= MaxStackItems ? stackalloc int[order.Length] : new int[order.Length];
                for (var index = 0; index < labellings; index++)
                {
                    order.CopyTo(listed);
                    var rest = index;
                    for (var run = 0; run < starts.Length; run++)
                    {
                        var block = Factorial(lengths[run]);
                        var pick = rest % block;
                        rest /= block;
                        var members = order.Slice(starts[run], lengths[run]);
                        WritePermutation(members, pick, listed.Slice(starts[run], lengths[run]));
                    }

                    ApplyOrder(listed, map);
                    candidate.Clear();
                    AppendNodePartitions(candidate, state, listed, map);
                    AppendOrderedMessages(candidate, state.Messages, map);
                    if (index != 0 && candidate.AsSpan().SequenceCompareTo(best.AsSpan()) >= 0)
                        continue;
                    best.Clear();
                    best.Append(candidate.AsSpan());
                }

                sb.Append(best.AsSpan());
            }

            private static void ApplyOrder(ReadOnlySpan<int> order, Span<int> map)
            {
                for (var i = 0; i < order.Length; i++)
                    map[order[i]] = i;
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

            private static int BuildTieRuns(IReadOnlyList<NodeState> nodes, ReadOnlySpan<int> order, Span<int> starts, Span<int> lengths)
            {
                var comparer = new NodeOrderComparer(nodes);
                var runCount = 0;
                var i = 0;
                while (i < order.Length)
                {
                    var j = i + 1;
                    while (j < order.Length && comparer.CompareKey(order[j - 1], order[j]) == 0)
                        j++;

                    if (j - i > 1)
                    {
                        starts[runCount] = i;
                        lengths[runCount] = j - i;
                        runCount++;
                    }

                    i = j;
                }

                return runCount;
            }

            private static int Factorial(int value)
            {
                var result = 1;
                for (var i = 2; i <= value; i++)
                    result *= i;

                return result;
            }

            private static void WritePermutation(ReadOnlySpan<int> members, int index, Span<int> destination)
            {
                var available = members.Length <= MaxStackItems ? stackalloc int[members.Length] : new int[members.Length];
                members.CopyTo(available);
                var remaining = members.Length;
                var rest = index;
                for (var i = 0; i < destination.Length; i++)
                {
                    var block = Factorial(remaining - 1);
                    var pick = block == 0 ? 0 : rest / block;
                    rest -= pick * block;
                    destination[i] = available[pick];
                    available[(pick + 1)..].CopyTo(available[pick..]);
                    remaining--;
                }
            }
        }

        /// <summary>Writes the retained hint evidence into the key.</summary>
        /// <remarks>
        /// It sits in its own type because the fingerprint writer is at the method ceiling the analyzer allows, and the
        /// responsibility is separable: this is one dimension of the key with its own ordering.
        /// </remarks>
        private static class HintHistoryKey
        {
            /// <summary>Appends the retained hint evidence, relabelled by the same map as the members it names.</summary>
            /// <param name="sb">The writer to append to.</param>
            /// <param name="emissions">The hint evidence retained so far.</param>
            /// <param name="map">The relabelling, empty when the state keeps its own labels.</param>
            /// <remarks>
            /// <para>
            /// The evidence names the member and the successors it named, so every id in it is relabelled like any other
            /// in the key. Written with the ids the state happens to hold, two states that differ only by renaming keep
            /// different keys, the exploration stops merging them, and the reduction silently costs the saving it exists
            /// to give.
            /// </para>
            /// <para>
            /// What the key writes about that evidence is the answer the policy checker reads from it, and not the records
            /// themselves. A retained record that already names a second successor is the violation, the check reports the
            /// first such record it meets, and the search keeps only that first report — so one marker says everything a
            /// key has to tell apart. A state holding such a record must not key like a state holding none. That was the
            /// defect: the marker was left out on the claim that the search stops at a policy violation, and the search
            /// does not stop. The successor's key is looked up in the seen set before the policy checker runs, and a state
            /// that breaks a policy is still a state, so it is recorded and explored; one that keyed like its clean twin
            /// was discarded as already seen, before anything had checked it.
            /// </para>
            /// <para>
            /// The record's own content stays out of the key, and what that costs is a merge, not a lost report. Two states
            /// agreeing on every other field produce the same successors, and both hold a violation the policy reports
            /// under one name, so exploring one of them reports what exploring the other would. Writing the content
            /// instead separates states the checker cannot tell apart: on <c lang="csharp">SmallTransferOneMoreTerm</c> the fault profile
            /// then exhausted its 300 000 states without reaching a fixed point, and lost the counterexample path with it.
            /// </para>
            /// </remarks>
            internal static void Append(FingerprintWriter sb, IReadOnlyList<ModelHintEmission> emissions, ReadOnlySpan<int> map)
            {
                var count = emissions.Count;
                var twoSuccessors = false;
                var order = count <= MaxStackItems ? stackalloc int[count] : new int[count];
                for (var i = 0; i < count; i++)
                {
                    var current = emissions[i];
                    if (current.SecondSuccessor >= 0)
                        twoSuccessors = true;

                    var j = i;
                    while (j > 0)
                    {
                        var previous = emissions[order[j - 1]];
                        if (Compare(in previous, in current, map) <= 0)
                            break;

                        order[j] = order[j - 1];
                        j--;
                    }

                    order[j] = i;
                }

                sb.Append('H');
                sb.Append(twoSuccessors ? '!' : '.');
                for (var i = 0; i < count; i++)
                {
                    var emission = emissions[order[i]];
                    if (emission.SecondSuccessor >= 0)
                        continue;

                    sb.Append(Mapped(map, emission.Sender));
                    sb.Append('@');
                    sb.Append(emission.Term);
                    sb.Append('>');
                    sb.Append(Mapped(map, emission.FirstSuccessor));
                    sb.Append("/-1;");
                }
            }

            private static int Compare(in ModelHintEmission a, in ModelHintEmission b, ReadOnlySpan<int> map)
            {
                var c = Mapped(map, a.Sender).CompareTo(Mapped(map, b.Sender));
                return c != 0 ? c : a.Term.CompareTo(b.Term);
            }
        }
    }
}
