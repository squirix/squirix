namespace Squirix.ProtocolModel;

internal static class ModelTraceMatch
{
    /// <summary>Decides whether a model state shows the trace point.</summary>
    /// <param name="previous">The state the model came from, or null for the initial state.</param>
    /// <param name="state">The state to match.</param>
    /// <param name="point">The trace point.</param>
    /// <returns>True when some leader of the state matches the point.</returns>
    internal static bool Matches(ClusterState? previous, ClusterState state, in ModelCommitTracePoint point)
    {
        for (var index = 0; index < state.Nodes.Count; index++)
        {
            if (Matches(previous, state, state.Nodes[index], in point))
                return true;
        }

        return false;
    }

    private static bool Matches(ClusterState? previous, ClusterState state, NodeState node, in ModelCommitTracePoint point) =>
        MatchesPosition(node, in point) && MatchesIdentity(node, in point) && MatchesRead(node, in point) &&
        (point.Acks == ModelCommitTracePoint.NotObserved || MatchesAcks(previous, state, node, point.Acks));

    private static bool MatchesPosition(NodeState node, in ModelCommitTracePoint point) =>
        node.Role == NodeRole.Leader && node.CurrentTerm == point.Term && node.LastLogIndex == point.LogIndex && node.CommitIndex == point.CommitIndex &&
        (point.AppliedIndex == ModelCommitTracePoint.NotObserved || node.AppliedIndex == point.AppliedIndex);

    private static bool MatchesIdentity(NodeState node, in ModelCommitTracePoint point) =>
        (point.LeaderId == ModelCommitTracePoint.NotObserved || node.Id == point.LeaderId) &&
        (point.LastLogTerm == ModelCommitTracePoint.NotObserved || node.LastLogTerm == point.LastLogTerm) &&
        (point.Votes == ModelCommitTracePoint.NotObserved || VoteMask.CountGranted(node.VotesGranted) <= point.Votes);

    private static bool MatchesRead(NodeState node, in ModelCommitTracePoint point) =>
        (point.ReadIndex == ModelCommitTracePoint.NotObserved || (node.ReadIndex == point.ReadIndex && node.ReadReady)) &&
        (point.ReadAcks == ModelCommitTracePoint.NotObserved || VoteMask.CountGranted(node.ReadAcks) <= point.ReadAcks);

    /// <summary>Decides whether the copies behind the leader's commit are at most the trace's acks.</summary>
    /// <param name="previous">The state the model came from, or null for the initial state.</param>
    /// <param name="state">The state to match.</param>
    /// <param name="node">The leader.</param>
    /// <param name="acks">The copies the trace saw.</param>
    /// <returns>True when the leader's commit index rose on this step and the model needed no more copies than the trace had.</returns>
    /// <remarks>
    /// MatchIndexes is shared by the whole cluster and a new leader resets it, so the copies are counted only on the step where this leader's
    /// commit index rises; on any later state the count says nothing about this commit.
    /// </remarks>
    private static bool MatchesAcks(ClusterState? previous, ClusterState state, NodeState node, int acks)
    {
        if (previous == null)
            return false;

        var before = previous.Nodes[node.Id];
        return before.Role == NodeRole.Leader && before.CommitIndex < node.CommitIndex && CountCopies(state, node, node.CommitIndex) <= acks;
    }

    private static int CountCopies(ClusterState state, NodeState leader, int index)
    {
        var leaderEntry = ModelTransitionUtil.FindEntry(leader.LogEntries, index);
        if (leaderEntry == null)
            return 0;

        var copies = 0;
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var stored = ModelTransitionUtil.FindEntry(state.Nodes[i].LogEntries, index);
            if (state.MatchIndexes[i] >= index && stored != null && stored.Value.Term == leaderEntry.Value.Term)
                copies++;
        }

        return copies;
    }
}
