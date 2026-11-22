using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Squirix.ProtocolModel;

internal static class SafetyChecker
{
    private const string ReadIndexInvariant = "ReadIndex";

    internal static SafetyViolation? Check(ClusterState state) =>
        CheckElectionSafety(state) ?? CheckCommittedSurvives(state) ?? CheckReadIndex(state);

    internal static string FormatCounterexampleJson(SafetyViolation violation, ClusterState state, IReadOnlyList<string>? counterexamplePath)
    {
        var sb = new StringBuilder(512);
        _ = sb.Append('{').Append("\"invariant\":");
        JsonText.AppendString(sb, violation.Invariant);
        _ = sb.Append(",\"detail\":");
        JsonText.AppendString(sb, violation.Detail);
        _ = sb.Append(",\"fingerprint\":");
        JsonText.AppendString(sb, violation.StateFingerprint);
        _ = sb.Append(",\"path\":[");
        if (counterexamplePath != null)
        {
            for (var p = 0; p < counterexamplePath.Count; p++)
            {
                if (p > 0)
                    _ = sb.Append(',');

                JsonText.AppendString(sb, counterexamplePath[p]);
            }
        }

        _ = sb.Append("],\"nodes\":[");
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            if (i > 0)
                _ = sb.Append(',');

            AppendNodeJson(sb, state.Nodes[i]);
        }

        _ = sb.Append("]}");
        return sb.ToString();
    }

    private static void AppendNodeJson(StringBuilder sb, NodeState n)
    {
        _ = sb.Append('{').Append("\"id\":").Append(n.Id.ToString(CultureInfo.InvariantCulture)).Append(",\"role\":");
        JsonText.AppendString(sb, FormatRole(n.Role));
        _ = sb.Append(",\"term\":").Append(n.CurrentTerm.ToString(CultureInfo.InvariantCulture)).Append(",\"votedFor\":").Append(n.VotedFor.ToString(CultureInfo.InvariantCulture))
              .Append(",\"commitIndex\":").Append(n.CommitIndex.ToString(CultureInfo.InvariantCulture)).Append(",\"appliedIndex\":")
              .Append(n.AppliedIndex.ToString(CultureInfo.InvariantCulture)).Append(",\"readIndex\":").Append(n.ReadIndex.ToString(CultureInfo.InvariantCulture))
              .Append(",\"readAcks\":").Append(n.ReadAcks.ToString(CultureInfo.InvariantCulture)).Append(",\"readReady\":").Append(n.ReadReady ? "true" : "false")
              .Append(",\"log\":[");
        for (var j = 0; j < n.LogEntries.Count; j++)
        {
            if (j > 0)
                _ = sb.Append(',');

            _ = sb.Append("{\"term\":").Append(n.LogEntries[j].Term.ToString(CultureInfo.InvariantCulture)).Append(",\"index\":")
                  .Append(n.LogEntries[j].Index.ToString(CultureInfo.InvariantCulture)).Append('}');
        }

        _ = sb.Append("]}");
    }

    private static SafetyViolation? CheckCommittedSurvives(ClusterState state) =>
        CheckStateMachineSafety(state) ?? CheckLeaderCompleteness(state);

    /// <summary>Checks that no term has had two distinct leaders elected, whether or not both still hold the role.</summary>
    /// <param name="state">The state to check.</param>
    /// <returns>The violation, or <see langword="null" /> when every term has had one leader.</returns>
    /// <remarks>
    /// Election safety is a property of what happened, not of what is currently true: reading the current roles checks
    /// that no two nodes are leaders at once, which misses the two-in-one-term case whenever the earlier leader has
    /// retired, because retiring makes it a follower and keeps its term. The evidence is therefore read from the
    /// recorded elections, which a leader's retirement does not remove.
    /// </remarks>
    private static SafetyViolation? CheckElectionSafety(ClusterState state)
    {
        var elected = state.ElectedLeaders;
        for (var i = 1; i < elected.Count; i++)
        {
            var previous = elected[i - 1];
            var current = elected[i];
            if (current.Term < previous.Term)
                return CheckUnorderedElections(state);

            if (current.Term == previous.Term && current.NodeId != previous.NodeId)
                return ElectionConflict(state, current.Term, previous.NodeId, current.NodeId);
        }

        return null;
    }

    private static SafetyViolation? CheckUnorderedElections(ClusterState state)
    {
        var elected = state.ElectedLeaders;
        var leadersByTerm = new Dictionary<int, int>();
        for (var i = 0; i < elected.Count; i++)
        {
            var election = elected[i];
            if (!leadersByTerm.TryGetValue(election.Term, out var other))
            {
                leadersByTerm[election.Term] = election.NodeId;
                continue;
            }

            if (other == election.NodeId)
                continue;

            return ElectionConflict(state, election.Term, other, election.NodeId);
        }

        return null;
    }

    private static SafetyViolation ElectionConflict(ClusterState state, int term, int first, int second)
    {
        var detail = "Two leaders in term " + term.ToString(CultureInfo.InvariantCulture) + ": " + first.ToString(CultureInfo.InvariantCulture) + " and " +
                     second.ToString(CultureInfo.InvariantCulture);
        return new SafetyViolation("ElectionSafety", detail, state.Fingerprint(false));
    }

    private static SafetyViolation? CheckLeaderCompleteness(ClusterState state)
    {
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (node.Role != NodeRole.Leader)
                continue;

            if (!FindMissingCommittedEntry(node, state.CommittedEntries, out var missing))
                continue;
            var detail = "Leader " + node.Id.ToString(CultureInfo.InvariantCulture) + " missing committed entry " + missing.EntryTerm.ToString(CultureInfo.InvariantCulture) + "@" +
                         missing.Index.ToString(CultureInfo.InvariantCulture);
            return new SafetyViolation("LeaderCompleteness", detail, state.Fingerprint(false));
        }

        return null;
    }

    private static SafetyViolation? CheckReadIndex(ClusterState state)
    {
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (!node.ReadReady)
                continue;

            var violation = CheckReadReadyNode(node, state);
            if (violation != null)
                return violation;
        }

        return null;
    }

    private static SafetyViolation? CheckReadReadyNode(NodeState node, ClusterState state)
    {
        if (node.Role != NodeRole.Leader)
            return new SafetyViolation(ReadIndexInvariant, "Non-leader marked read ready", state.Fingerprint(false));

        if (node.ReadIndex <= 0)
            return new SafetyViolation(ReadIndexInvariant, "Read ready without pending read index", state.Fingerprint(false));

        var majority = (state.Nodes.Count / 2) + 1;
        var quorumUnconfirmed = VoteMask.CountGranted(node.ReadAcks) < majority;
        var notApplied = node.AppliedIndex < node.ReadIndex;
        return (quorumUnconfirmed, notApplied) switch
        {
            (true, _) => new SafetyViolation(ReadIndexInvariant, "Read served without current-term majority confirm", state.Fingerprint(false)),
            (false, true) => new SafetyViolation(ReadIndexInvariant, "Read served before appliedIndex >= readIndex", state.Fingerprint(false)),
            (false, false) => null,
        };
    }

    private static SafetyViolation? CheckStateMachineSafety(ClusterState state)
    {
        var maxIndex = MaxCommitIndex(state);
        for (var index = 1; index <= maxIndex; index++)
        {
            if (!TryFindConflictingTerm(state, index))
                continue;

            var detail = "Conflicting committed terms at index " + index.ToString(CultureInfo.InvariantCulture);
            return new SafetyViolation("StateMachineSafety", detail, state.Fingerprint(false));
        }

        return null;
    }

    private static bool ContainsEntry(IReadOnlyList<LogEntry> log, int index, int term)
    {
        for (var i = 0; i < log.Count; i++)
        {
            if (log[i].Index == index && log[i].Term == term)
                return true;
        }

        return false;
    }

    private static LogEntry? FindEntry(IReadOnlyList<LogEntry> log, int index)
    {
        for (var i = 0; i < log.Count; i++)
        {
            if (log[i].Index == index)
                return log[i];
        }

        return null;
    }

    private static bool FindMissingCommittedEntry(NodeState node, IReadOnlyList<ModelCommitDecision> committed, out ModelCommitDecision missing)
    {
        for (var i = 0; i < committed.Count; i++)
        {
            var entry = committed[i];

            // Entry term and decision term differ when a later leader commits an earlier-term prefix.
            // Only decisions made by this leadership term or earlier constrain the leader.
            if (entry.CommitTerm > node.CurrentTerm || ContainsEntry(node.LogEntries, entry.Index, entry.EntryTerm))
                continue;

            missing = entry;
            return true;
        }

        missing = default;
        return false;
    }

    private static string FormatRole(NodeRole role)
    {
        return role switch
        {
            NodeRole.Follower => "Follower",
            NodeRole.Candidate => "Candidate",
            NodeRole.Leader => "Leader",
            _ => throw new ArgumentOutOfRangeException(nameof(role), role, "Unsupported enum value."),
        };
    }

    private static int MaxCommitIndex(ClusterState state)
    {
        var maxIndex = 0;
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            if (state.Nodes[i].CommitIndex > maxIndex)
                maxIndex = state.Nodes[i].CommitIndex;
        }

        return maxIndex;
    }

    private static bool TryFindConflictingTerm(ClusterState state, int index)
    {
        int? seenTerm = null;
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (node.CommitIndex < index)
                continue;

            var entry = FindEntry(node.LogEntries, index);
            if (entry == null)
                continue;

            if (seenTerm == null)
            {
                seenTerm = entry.Value.Term;
                continue;
            }

            if (seenTerm.Value == entry.Value.Term)
                continue;

            return true;
        }

        return false;
    }
}
