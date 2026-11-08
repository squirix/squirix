using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Squirix.ProtocolModel;

/// <summary>Checks what the transfer protocol states about itself, which are not properties of the data.</summary>
/// <remarks>
/// The safety invariants ask whether two leaders were elected in one term, whether a committed entry was lost, or whether
/// a read was served too early. These ask whether a step was allowed at all: a hint may leave only a leader that has
/// already stepped down in the term it names, and it names exactly one successor. Both can be false while every safety
/// invariant holds, and that combination is the interesting one, so the two are checked and reported apart.
/// </remarks>
internal static class ProtocolPolicyChecker
{
    /// <summary>The policy that a hint leaves a leader that has already retired in the term the hint names.</summary>
    internal const string HintSourceRetired = "TransferHintSourceRetired";

    /// <summary>The policy that a hint names exactly one successor, so a retirement cannot put two hints from one term in flight.</summary>
    internal const string HintNamesOneSuccessor = "TransferHintNamesOneSuccessor";

    /// <summary>Gets every rule this checker can report, which is every policy report the CLI can write.</summary>
    /// <remarks>
    /// A report is named after the rule it carries, so this set is exactly the set of report names the CLI can produce.
    /// A consumer that has to recognise its own reports reads the names from here instead of matching a pattern, because
    /// a pattern also matches files this program did not write.
    /// </remarks>
    internal static IReadOnlyList<string> PolicyNames { get; } = [HintSourceRetired, HintNamesOneSuccessor];

    /// <summary>Returns the report file name that carries one rule's counterexample.</summary>
    /// <param name="policy">The rule name, one of <see cref="PolicyNames" />.</param>
    /// <returns>The file name, with no directory part.</returns>
    internal static string PolicyReportFileName(string policy) => "policy-" + policy + ".json";

    /// <summary>Returns the first policy the state breaks, or <see langword="null" /> when it breaks none.</summary>
    /// <param name="state">The state to check.</param>
    /// <returns>The violation, with the event that produced it.</returns>
    internal static PolicyViolation? Check(ClusterState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message.Kind == MsgKind.TransferHint)
            {
                var violation = CheckHint(state, in message);
                if (violation != null)
                    return violation;
            }
        }

        return CheckHintHistory(state);
    }

    /// <summary>Renders a policy counterexample as JSON, with the event that broke the rule and the path that reached it.</summary>
    /// <param name="violation">The policy the run broke.</param>
    /// <param name="state">The state the step produced.</param>
    /// <param name="path">The fingerprints from the initial state to that one, or <see langword="null" /> when unknown.</param>
    /// <returns>The document.</returns>
    internal static string FormatCounterexampleJson(PolicyViolation violation, ClusterState state, IReadOnlyList<string>? path)
    {
        ArgumentNullException.ThrowIfNull(violation);
        ArgumentNullException.ThrowIfNull(state);
        var sb = new StringBuilder(384);
        _ = sb.Append('{').Append("\"policy\":");
        JsonText.AppendString(sb, violation.Policy);
        _ = sb.Append(",\"detail\":");
        JsonText.AppendString(sb, violation.Detail);
        _ = sb.Append(",\"event\":");
        JsonText.AppendString(sb, violation.EventTrace);
        _ = sb.Append(",\"fingerprint\":");
        JsonText.AppendString(sb, violation.StateFingerprint);
        _ = sb.Append(",\"path\":[");
        if (path != null)
            AppendPath(sb, path);

        _ = sb.Append("],\"nodes\":[");
        for (var i = 0; i < state.Nodes.Count; i++)
        {
            if (i > 0)
                _ = sb.Append(',');

            AppendNodeJson(sb, state.Nodes[i]);
        }

        _ = sb.Append("],\"messages\":[");
        for (var i = 0; i < state.Messages.Count; i++)
        {
            if (i > 0)
                _ = sb.Append(',');

            _ = sb.Append("{\"kind\":");
            JsonText.AppendString(sb, KindName(state.Messages[i].Kind));
            _ = sb.Append(",\"from\":").Append(Int(state.Messages[i].From))
                  .Append(",\"to\":").Append(Int(state.Messages[i].To))
                  .Append(",\"term\":").Append(Int(state.Messages[i].Term)).Append('}');
        }

        _ = sb.Append("]}");
        return sb.ToString();
    }

    /// <summary>Names a message kind for a diagnostic document.</summary>
    /// <param name="kind">The kind.</param>
    /// <returns>The name.</returns>
    internal static string KindName(MsgKind kind) => kind switch
    {
        MsgKind.RequestVote => "RequestVote",
        MsgKind.VoteResponse => "VoteResponse",
        MsgKind.AppendEntries => "AppendEntries",
        MsgKind.AppendResponse => "AppendResponse",
        MsgKind.ReadIndexRequest => "ReadIndexRequest",
        MsgKind.ReadIndexResponse => "ReadIndexResponse",
        MsgKind.TransferHint => "TransferHint",
        _ => "Other",
    };

    /// <summary>Names a role for a diagnostic document.</summary>
    /// <param name="role">The role.</param>
    /// <returns>The name.</returns>
    internal static string RoleName(NodeRole role) => role switch
    {
        NodeRole.Follower => "Follower",
        NodeRole.Candidate => "Candidate",
        NodeRole.Leader => "Leader",
        _ => "Unknown",
    };

    private static PolicyViolation? CheckHint(ClusterState state, in InFlightMessage message)
    {
        var sender = state.Nodes[message.From];

        // Retiring is what earns the right to name a successor: the leader steps down to Follower in the same term and
        // only then does the hint exist. A leader that is still the leader of the term it names has not retired, so the
        // step is not the protocol's, whatever the votes do afterwards.
        if (sender.Role == NodeRole.Leader && sender.CurrentTerm == message.Term)
            return new PolicyViolation(HintSourceRetired, StillServingDetail(in message), state.Fingerprint(false), FormatEvent(state, in message));

        if (message.To == message.From)
            return new PolicyViolation(HintNamesOneSuccessor, NamedItselfDetail(in message), state.Fingerprint(false), FormatEvent(state, in message));

        var named = CountSuccessorsNamed(state, message.From, message.Term);
        return named > 1
            ? new PolicyViolation(HintNamesOneSuccessor, TwoSuccessorsDetail(in message, named), state.Fingerprint(false), FormatEvent(state, in message))
            : null;
    }

    /// <summary>Returns the first term in which one member named two different successors, or <see langword="null" />.</summary>
    /// <param name="state">The state to check.</param>
    /// <returns>The violation, naming the member, the term and both successors.</returns>
    /// <remarks>
    /// This reads the retained evidence rather than the message list, because the list only holds what is still in
    /// flight. A hint is delivered or dropped before the next step, so a member that named one successor, let that hint
    /// leave and then named another in the same term leaves no message behind: counting the list would answer "how many
    /// hints are in flight", which is not the question this policy asks.
    /// </remarks>
    private static PolicyViolation? CheckHintHistory(ClusterState state)
    {
        var emissions = state.HintEmissions;
        for (var i = 0; i < emissions.Count; i++)
        {
            var emission = emissions[i];
            if (emission.SecondSuccessor < 0)
                continue;

            return new PolicyViolation(
                HintNamesOneSuccessor,
                TwoSuccessorsInTermDetail(in emission),
                state.Fingerprint(false),
                TwoHintsEvent(in emission));
        }

        return null;
    }

    private static string TwoSuccessorsInTermDetail(in ModelHintEmission emission) =>
        "Node " + Int(emission.Sender) + " named " + Int(emission.FirstSuccessor) + " and " +
        Int(emission.SecondSuccessor) + " as different successors in term " + Int(emission.Term) +
        ", so the retirement did not name one";

    private static string TwoHintsEvent(in ModelHintEmission emission) =>
        "TransferHint(from=" + Int(emission.Sender) + ",to=" + Int(emission.FirstSuccessor) + ",term=" +
        Int(emission.Term) + ") then TransferHint(from=" + Int(emission.Sender) + ",to=" +
        Int(emission.SecondSuccessor) + ",term=" + Int(emission.Term) + ")";

    private static void AppendNodeJson(StringBuilder sb, NodeState node)
    {
        _ = sb.Append("{\"id\":").Append(Int(node.Id)).Append(",\"role\":");
        JsonText.AppendString(sb, RoleName(node.Role));
        _ = sb.Append(",\"term\":").Append(Int(node.CurrentTerm))
              .Append(",\"votedFor\":").Append(Int(node.VotedFor))
              .Append(",\"commitIndex\":").Append(Int(node.CommitIndex)).Append('}');
    }

    private static void AppendPath(StringBuilder sb, IReadOnlyList<string> path)
    {
        for (var i = 0; i < path.Count; i++)
        {
            if (i > 0)
                _ = sb.Append(',');

            JsonText.AppendString(sb, path[i]);
        }
    }

    /// <summary>Counts the distinct members the hints from one source in one term name.</summary>
    /// <param name="state">The state.</param>
    /// <param name="sender">The retiring leader.</param>
    /// <param name="term">The term it retired in.</param>
    /// <returns>The number of distinct successors named.</returns>
    /// <remarks>
    /// Distinct members, not messages: the explorer models a duplicated delivery, so the same hint can be in flight twice
    /// after the network copied it, and a copy names the same successor as its original. Counting messages would report
    /// that copy as a second successor, which is the defect this check exists to find, not the event that produced it.
    /// </remarks>
    private static int CountSuccessorsNamed(ClusterState state, int sender, int term)
    {
        var named = 0;
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var message = state.Messages[i];
            if (message.Kind != MsgKind.TransferHint || message.From != sender || message.Term != term)
                continue;

            var first = true;
            for (var j = 0; j < i; j++)
            {
                var earlier = state.Messages[j];
                if (earlier.Kind == MsgKind.TransferHint && earlier.From == sender && earlier.Term == term && earlier.To == message.To)
                {
                    first = false;
                    break;
                }
            }

            if (first)
                named++;
        }

        return named;
    }

    private static string FormatEvent(ClusterState state, in InFlightMessage message)
    {
        var sb = new StringBuilder(96);
        _ = sb.Append("TransferHint(from=").Append(Int(message.From))
              .Append(",to=").Append(Int(message.To))
              .Append(",term=").Append(Int(message.Term))
              .Append(",senderRole=").Append(RoleName(state.Nodes[message.From].Role))
              .Append(",senderTerm=").Append(Int(state.Nodes[message.From].CurrentTerm))
              .Append(')');
        return sb.ToString();
    }

    private static string Int(int value) => value.ToString(CultureInfo.InvariantCulture);

    private static string NamedItselfDetail(in InFlightMessage message) =>
        "Node " + Int(message.From) + " named itself as the successor in term " + Int(message.Term);

    private static string StillServingDetail(in InFlightMessage message) =>
        "Node " + Int(message.From) + " sent a transfer hint in term " + Int(message.Term) + " while it is still the leader of that term, so it did not retire";

    private static string TwoSuccessorsDetail(in InFlightMessage message, int count) =>
        "Node " + Int(message.From) + " named " + Int(count) + " different successors in term " + Int(message.Term) + ", so the retirement did not name one";
}
