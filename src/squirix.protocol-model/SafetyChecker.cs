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

    private static SafetyViolation? CheckCommittedSurvives(ClusterState state)
    {
        var conflict = CheckStateMachineSafety(state);
        if (conflict != null)
            return conflict;

        var committed = CollectCommitted(state);
        return CheckLeaderCompleteness(state, committed);
    }

    private static SafetyViolation? CheckElectionSafety(ClusterState state)
    {
        var leadersByTerm = new Dictionary<int, int>();
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (node.Role != NodeRole.Leader)
                continue;

            if (leadersByTerm.TryGetValue(node.CurrentTerm, out var other))
            {
                var detail = "Two leaders in term " + node.CurrentTerm.ToString(CultureInfo.InvariantCulture) + ": " + other.ToString(CultureInfo.InvariantCulture) + " and " +
                             node.Id.ToString(CultureInfo.InvariantCulture);
                return new SafetyViolation("ElectionSafety", detail, state.Fingerprint(false));
            }

            leadersByTerm[node.CurrentTerm] = node.Id;
        }

        return null;
    }

    private static SafetyViolation? CheckLeaderCompleteness(ClusterState state, Dictionary<int, int> committed)
    {
        var maxTerm = MaxTerm(state);
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (node.Role != NodeRole.Leader || node.CurrentTerm != maxTerm)
                continue;

            if (!FindMissingCommittedEntry(node, committed, out var missing))
                continue;
            var detail = "Leader " + node.Id.ToString(CultureInfo.InvariantCulture) + " missing committed entry " + missing.Value.ToString(CultureInfo.InvariantCulture) + "@" +
                         missing.Key.ToString(CultureInfo.InvariantCulture);
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
        var fingerprint = state.Fingerprint(false);
        return (quorumUnconfirmed, notApplied) switch
        {
            (true, _) => new SafetyViolation(ReadIndexInvariant, "Read served without current-term majority confirm", fingerprint),
            (false, true) => new SafetyViolation(ReadIndexInvariant, "Read served before appliedIndex >= readIndex", fingerprint),
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

    private static Dictionary<int, int> CollectCommitted(ClusterState state)
    {
        // An entry is committed when any node's commitIndex covers it; the term comes from that node's log.
        // A node's commitIndex rises only through its own leader commit rule (a majority stored an entry of its
        // current term) or a leader's repair, so a single commitIndex is already a commit decision.
        // StateMachineSafety, checked first, keeps all nodes that cover an index in agreement on its term.
        var committed = new Dictionary<int, int>();
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            for (var j = 0; j < node.LogEntries.Count; j++)
            {
                var entry = node.LogEntries[j];
                if (entry.Index <= node.CommitIndex)
                    committed[entry.Index] = entry.Term;
            }
        }

        return committed;
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

    private static bool FindMissingCommittedEntry(NodeState node, Dictionary<int, int> committed, out KeyValuePair<int, int> missing)
    {
        foreach (var pair in committed)
        {
            if (ContainsEntry(node.LogEntries, pair.Key, pair.Value))
                continue;

            missing = pair;
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

    private static int MaxTerm(ClusterState state)
    {
        var maxTerm = 0;
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            if (state.Nodes[i].CurrentTerm > maxTerm)
                maxTerm = state.Nodes[i].CurrentTerm;
        }

        return maxTerm;
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
