using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Squirix.ProtocolModel;

internal static class SafetyChecker
{
    private const string ReadIndexInvariant = "ReadIndex";

    internal static SafetyViolation? Check(ClusterState state, BrokenMode broken)
    {
        _ = broken;
        return CheckElectionSafety(state) ?? CheckCommittedSurvives(state) ?? CheckCurrentTermCommit(state) ?? CheckReadIndex(state);
    }

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
        if (counterexamplePath is not null)
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
        if (conflict is not null)
            return conflict;

        var committed = CollectMajorityCommitted(state);
        return CheckLeaderCompleteness(state, committed);
    }

    private static SafetyViolation? CheckCurrentTermCommit(ClusterState state)
    {
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (!node.BadOldCommit)
                continue;

            var detail = "Leader " + node.Id.ToString(CultureInfo.InvariantCulture) + " advanced commit over old-term entries without a current-term commit";
            return new SafetyViolation("CurrentTermCommit", detail, state.Fingerprint(false));
        }

        return null;
    }

    private static SafetyViolation? CheckElectionSafety(ClusterState state)
    {
        var leadersByTerm = new Dictionary<int, int>();
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (node.Role is not NodeRole.Leader)
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
            if (node.Role is not NodeRole.Leader || node.CurrentTerm != maxTerm)
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
            if (violation is not null)
                return violation;
        }

        return null;
    }

    private static SafetyViolation? CheckReadReadyNode(NodeState node, ClusterState state)
    {
        if (node.Role is not NodeRole.Leader)
            return new SafetyViolation(ReadIndexInvariant, "Non-leader marked read ready", state.Fingerprint(false));

        if (node.ReadIndex <= 0)
            return new SafetyViolation(ReadIndexInvariant, "Read ready without pending read index", state.Fingerprint(false));

        var majority = (state.Nodes.Count / 2) + 1;
        if (VoteMask.CountGranted(node.ReadAcks) < majority)
            return new SafetyViolation(ReadIndexInvariant, "Read served without current-term majority confirm", state.Fingerprint(false));

        if (node.AppliedIndex < node.ReadIndex)
            return new SafetyViolation(ReadIndexInvariant, "Read served before appliedIndex >= readIndex", state.Fingerprint(false));

        return null;
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

    private static Dictionary<int, int> CollectMajorityCommitted(ClusterState state)
    {
        // An entry is treated as committed only when a majority of nodes both store it and have
        // a commitIndex covering it. Local commitIndex alone is not enough (avoids false positives).
        var majority = (state.Nodes.Count / 2) + 1;
        var committed = new Dictionary<int, int>();
        var maxIndex = MaxCommitIndex(state);
        for (var index = 1; index <= maxIndex; index++)
            RecordMajorityTerm(state, index, majority, committed);

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

    private static Dictionary<int, int> CountTermOccurrences(ClusterState state, int index)
    {
        var termCounts = new Dictionary<int, int>();
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            var node = state.Nodes[i];
            if (node.CommitIndex < index)
                continue;

            var entry = FindEntry(node.LogEntries, index);
            if (entry is null)
                continue;

            _ = termCounts.TryGetValue(entry.Value.Term, out var count);
            termCounts[entry.Value.Term] = count + 1;
        }

        return termCounts;
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

    private static void RecordMajorityTerm(ClusterState state, int index, int majority, Dictionary<int, int> committed)
    {
        foreach (var pair in CountTermOccurrences(state, index))
        {
            if (pair.Value < majority)
                continue;

            committed[index] = pair.Key;
            return;
        }
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
            if (entry is null)
                continue;

            if (seenTerm is null)
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
