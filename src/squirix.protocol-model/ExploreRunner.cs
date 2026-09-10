using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

internal static class ExploreRunner
{
    private const int ExitBrokenWithoutViolation = 3;
    private const int ExitBudgetExhausted = 4;

    private const int ExitSuccess = 0;
    private const int ExitViolation = 2;

    private enum SuccessorOutcome
    {
        Enqueued = 0,
        BudgetExhausted = 1,
        Completed = 2,
    }

    internal static string ModelVersionHash { get; } = ComputeModelVersionHash();

    internal static bool AcceptsCommitTrace(IReadOnlyList<ModelCommitTracePoint> trace)
    {
        ArgumentNullException.ThrowIfNull(trace);
        if (trace.Count == 0)
            return false;

        var profile = ExploreProfile.SmallCommit(false);
        var initial = ClusterState.CreateInitial(profile.ReplicaCount);
        var initialIndex = AdvanceTrace(initial, trace, 0);
        return RunTraceSearch(profile, trace, initial, initialIndex);
    }

    internal static ExploreResult Run(ExploreProfile profile, BrokenMode broken) => StateExplorer.Explore(profile, broken);

    internal static async Task<int> RunCliAsync(string profileName, string outputDir, BrokenMode broken)
    {
        _ = Directory.CreateDirectory(outputDir);
        var results = CollectResults(profileName, broken);
        var aggregate = AggregateResults(results);
        var summary = new SummaryContent(profileName, broken, aggregate.TotalStates, aggregate.TotalTransitions, aggregate.FirstViolation, aggregate.AllFixedPoint);
        await WriteSummaryAsync(outputDir, summary, CancellationToken.None).ConfigureAwait(false);

        if (aggregate.FirstViolation == null || aggregate.FirstState == null)
        {
            var staleCounterexample = Path.Join(outputDir, "counterexample.json");
            if (File.Exists(staleCounterexample))
                File.Delete(staleCounterexample);

            return ExitCode(broken, aggregate.FirstViolation, aggregate.AllFixedPoint);
        }

        var path = FindPath(results, aggregate.FirstViolation);
        var json = SafetyChecker.FormatCounterexampleJson(aggregate.FirstViolation, aggregate.FirstState, path);
        await File.WriteAllTextAsync(Path.Join(outputDir, "counterexample.json"), json, Encoding.UTF8, CancellationToken.None).ConfigureAwait(false);

        return ExitCode(broken, aggregate.FirstViolation, aggregate.AllFixedPoint);
    }

    private static int AdvanceTrace(ClusterState state, IReadOnlyList<ModelCommitTracePoint> trace, int traceIndex)
    {
        while (traceIndex < trace.Count && ContainsTracePoint(state, trace[traceIndex]))
            traceIndex++;

        return traceIndex;
    }

    private static ResultAggregate AggregateResults(List<(ExploreProfile Profile, ExploreResult Result)> results)
    {
        SafetyViolation? firstViolation = null;
        ClusterState? firstState = null;
        var totalStates = 0;
        var totalTransitions = 0;
        var allFixedPoint = true;
        for (var i = 0; i < results.Count; i++)
        {
            totalStates += results[i].Result.StatesVisited;
            totalTransitions += results[i].Result.TransitionsApplied;
            if (!results[i].Result.FixedPointReached && results[i].Result.Violation == null)
                allFixedPoint = false;

            if (firstViolation != null || results[i].Result.Violation == null)
                continue;
            firstViolation = results[i].Result.Violation;
            firstState = results[i].Result.ViolatingState;
        }

        return new ResultAggregate(totalStates, totalTransitions, firstViolation, firstState, allFixedPoint);
    }

    private static void CollectFullProfiles(BrokenMode broken, List<(ExploreProfile Profile, ExploreResult Result)> results)
    {
        // Elections RF=2..5 up to 3 terms; commit/read bounds from the milestone profile.
        for (var rf = 2; rf <= 5; rf++)
        {
            var election = ExploreProfile.ForReplicaCount(rf, 3, 1, 4, 0, true, true);
            results.Add((election, Run(election, broken)));

            var commit = ExploreProfile.ForReplicaCount(rf, 3, 3, 4, 0, true, true);
            results.Add((commit, Run(commit, broken)));

            var read = ExploreProfile.ForReplicaCount(rf, 2, 2, 4, 1, false, true);
            results.Add((read, Run(read, broken)));
        }
    }

    private static List<(ExploreProfile Profile, ExploreResult Result)> CollectResults(string profileName, BrokenMode broken)
    {
        var results = new List<(ExploreProfile Profile, ExploreResult Result)>();
        if (string.Equals(profileName, "full", StringComparison.OrdinalIgnoreCase))
        {
            CollectFullProfiles(broken, results);
            return results;
        }

        var profile = ExploreProfile.ForCli(profileName, true);
        results.Add((profile, Run(profile, broken)));
        return results;
    }

    private static string ComputeModelVersionHash()
    {
        // Manual semantics fingerprint (Assembly/MVID banned by RS0030). Bump when transitions or invariants change.
        const string semantic = "squirix-protocol-model-v1-raft-safety-match-entry-aware-commit-json";
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(semantic));
        var sb = new StringBuilder(16);
        for (var i = 0; i < 8; i++)
            _ = sb.Append(bytes[i].ToString("x2", CultureInfo.InvariantCulture));

        return sb.ToString();
    }

    private static bool ContainsTracePoint(ClusterState state, ModelCommitTracePoint point)
    {
        for (var index = 0; index < state.Nodes.Count; index++)
        {
            var node = state.Nodes[index];
            if (node.Role == NodeRole.Leader && node.CurrentTerm == point.Term && node.LastLogIndex == point.LogIndex && node.CommitIndex == point.CommitIndex &&
                node.AppliedIndex == point.AppliedIndex)
                return true;
        }

        return false;
    }

    private static int ExitCode(BrokenMode broken, SafetyViolation? firstViolation, bool allFixedPoint)
    {
        // Success = clean exhaustion or the injected break manifesting; 3 = injected break stayed silent; 4 = hit documented MaxStates (residual risk, not a counterexample).
        return (broken != BrokenMode.None, firstViolation != null, allFixedPoint) switch
        {
            (true, true, _) => ExitSuccess,
            (true, false, _) => ExitBrokenWithoutViolation,
            (false, true, _) => ExitViolation,
            (false, false, true) => ExitSuccess,
            (false, false, false) => ExitBudgetExhausted,
        };
    }

    private static IReadOnlyList<string>? FindPath(List<(ExploreProfile Profile, ExploreResult Result)> results, SafetyViolation violation)
    {
        for (var i = 0; i < results.Count; i++)
        {
            var result = results[i].Result;
            if (result.Violation != null && string.Equals(result.Violation.Invariant, violation.Invariant, StringComparison.Ordinal) && string.Equals(
                    result.Violation.StateFingerprint,
                    violation.StateFingerprint,
                    StringComparison.Ordinal))
                return result.CounterexamplePaths;
        }

        return null;
    }

    private static string FormatBrokenMode(BrokenMode broken)
    {
        return broken switch
        {
            BrokenMode.None => "None",
            BrokenMode.Vote => "Vote",
            BrokenMode.CurrentTermCommit => "CurrentTermCommit",
            BrokenMode.ReadIndex => "ReadIndex",
            _ => throw new ArgumentOutOfRangeException(nameof(broken), broken, "Unsupported enum value."),
        };
    }

    private static SuccessorOutcome ProcessSuccessors(TraceSearchWork work, TraceSearchState current)
    {
        var budgetExhausted = false;
        for (var index = 0; index < work.Successors.Count; index++)
        {
            var next = work.Successors[index];
            var nextTraceIndex = AdvanceTrace(next, work.TracePoints, current.TraceIndex);
            if (nextTraceIndex == work.TracePoints.Count)
                return SuccessorOutcome.Completed;

            var fingerprint = TraceFingerprint(next, nextTraceIndex);
            if (work.Seen.Contains(fingerprint))
                continue;

            if (work.Seen.Count >= work.MaxStates)
            {
                budgetExhausted = true;
                continue;
            }

            _ = work.Seen.Add(fingerprint);

            work.Queue.Enqueue(new TraceSearchState(next, nextTraceIndex));
        }

        return budgetExhausted ? SuccessorOutcome.BudgetExhausted : SuccessorOutcome.Enqueued;
    }

    private static bool RunTraceSearch(ExploreProfile profile, IReadOnlyList<ModelCommitTracePoint> trace, ClusterState initial, int initialIndex)
    {
        var work = new TraceSearchWork(trace, checked(profile.MaxStates * (trace.Count + 1)));
        work.Queue.Enqueue(new TraceSearchState(initial, initialIndex));
        _ = work.Seen.Add(TraceFingerprint(initial, initialIndex));

        var omittedUnique = false;
        while (work.Queue.Count > 0)
        {
            var current = work.Queue.Dequeue();
            if (current.TraceIndex == trace.Count)
                return true;

            ModelTransitions.CollectSuccessors(current.State, profile, BrokenMode.None, work.Successors);
            var outcome = ProcessSuccessors(work, current);
            if (outcome == SuccessorOutcome.Completed)
                return true;

            if (outcome == SuccessorOutcome.BudgetExhausted)
                omittedUnique = true;
        }

        return omittedUnique ? throw new TraceSearchBudgetExhaustedException(work.MaxStates, work.Seen.Count) : false;
    }

    private static string TraceFingerprint(ClusterState state, int traceIndex) => $"{traceIndex.ToString(CultureInfo.InvariantCulture)}:{state.Fingerprint(false)}";

    private static Task WriteSummaryAsync(string outputDir, SummaryContent content, CancellationToken cancellationToken)
    {
        var sb = new StringBuilder(256);
        _ = sb.Append('{').Append("\"modelVersionHash\":");
        JsonText.AppendString(sb, ModelVersionHash);
        _ = sb.Append(",\"profile\":");
        JsonText.AppendString(sb, content.ProfileName);
        _ = sb.Append(",\"broken\":");
        JsonText.AppendString(sb, FormatBrokenMode(content.Broken));
        _ = sb.Append(",\"statesVisited\":").Append(content.States.ToString(CultureInfo.InvariantCulture)).Append(",\"transitionsApplied\":")
              .Append(content.Transitions.ToString(CultureInfo.InvariantCulture)).Append(",\"fixedPointReached\":").Append(content.FixedPointReached ? "true" : "false")
              .Append(",\"violation\":");
        if (content.Violation == null)
        {
            _ = sb.Append("null");
        }
        else
        {
            _ = sb.Append('{').Append("\"invariant\":");
            JsonText.AppendString(sb, content.Violation.Invariant);
            _ = sb.Append(",\"detail\":");
            JsonText.AppendString(sb, content.Violation.Detail);
            _ = sb.Append('}');
        }

        _ = sb.Append('}');
        return File.WriteAllTextAsync(Path.Join(outputDir, "summary.json"), sb.ToString(), Encoding.UTF8, cancellationToken);
    }

    [Immutable]
    private readonly record struct ResultAggregate(int TotalStates, int TotalTransitions, SafetyViolation? FirstViolation, ClusterState? FirstState, bool AllFixedPoint);

    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct SummaryContent(string ProfileName, BrokenMode Broken, int States, int Transitions, SafetyViolation? Violation, bool FixedPointReached);

    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct TraceSearchState(ClusterState State, int TraceIndex);

    private static class ModelTransitions
    {
        internal static void CollectSuccessors(ClusterState state, ExploreProfile profile, BrokenMode broken, List<ClusterState> output)
        {
            output.Clear();
            CollectElections(state, profile, output);
            CollectClientProposals(state, profile, output);
            CollectReadIndexes(state, profile, broken, output);
            CollectDeliveries(state, profile, broken, output);
            CollectDrops(state, output);
            CollectDuplicates(state, profile, output);
            CollectApplyAdvances(state, profile, broken, output);
            if (broken is BrokenMode.CurrentTermCommit)
                CollectBrokenOldTermCommits(state, output);

            if (profile.AllowPartition)
                CollectPartitions(state, output);

            if (profile.AllowCrash)
                CollectCrashes(state, output);
        }

        private static void AddHealedPartition(ClusterState state, IReadOnlyList<int> parts, List<ClusterState> output)
        {
            var healed = ModelTransitionUtil.CloneInts(parts);
            for (var i = 0; i < healed.Length; i++)
                healed[i] = 0;

            output.Add(state.WithPartitions(healed));
        }

        private static void AddIsolationPartitions(ClusterState state, IReadOnlyList<int> parts, List<ClusterState> output)
        {
            // Isolate each replica — permutation-invariant under symmetry reduction.
            for (var isolated = 0; isolated < parts.Count; isolated++)
            {
                var split = ModelTransitionUtil.CloneInts(parts);
                for (var i = 0; i < split.Length; i++)
                    split[i] = i == isolated ? 1 : 0;

                output.Add(state.WithPartitions(split));
            }
        }

        private static void CollectApplyAdvances(ClusterState state, ExploreProfile profile, BrokenMode broken, List<ClusterState> output)
        {
            for (var i = 0; i < state.Nodes.Count; i++)
            {
                var node = state.Nodes[i];
                if (node.AppliedIndex >= node.CommitIndex)
                    continue;
                var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
                var applied = node.AppliedIndex + 1;
                var ready = ComputeReadReady(node, applied, profile, broken) || node.ReadReady;

                nodes[i] = ModelTransitionUtil.Patch(node, new NodePatch { AppliedIndex = applied, ReadReady = ready });
                output.Add(state.WithNodes(nodes));
            }
        }

        private static void CollectBrokenOldTermCommits(ClusterState state, List<ClusterState> output)
        {
            for (var i = 0; i < state.Nodes.Count; i++)
            {
                var leader = state.Nodes[i];
                if (leader.Role != NodeRole.Leader || !TryFindOldestOldTermEntry(leader, out var old))
                    continue;

                var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
                nodes[i] = ModelTransitionUtil.Patch(leader, new NodePatch { CommitIndex = old.Index, BadOldCommit = true });
                output.Add(state.WithNodes(nodes));
            }
        }

        private static void CollectClientProposals(ClusterState state, ExploreProfile profile, List<ClusterState> output)
        {
            for (var i = 0; i < state.Nodes.Count; i++)
            {
                if (!TryBuildClientProposal(state, profile, i, out var after))
                    continue;

                output.Add(after);
            }
        }

        private static void CollectCrashes(ClusterState state, List<ClusterState> output)
        {
            for (var i = 0; i < state.Nodes.Count; i++)
            {
                var node = state.Nodes[i];

                // Crash after durable writes: keep term/vote/log/commit; reset volatile.
                var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
                var runtime = NodeRuntime.Create(node.CommitIndex, 0, 0, 0, 0, false, false);
                nodes[i] = new NodeState(node.Id, NodeRole.Follower, node.CurrentTerm, node.VotedFor, node.LogEntries, runtime);
                var match = ModelTransitionUtil.CloneInts(state.MatchIndexes);
                match[i] = 0;
                output.Add(state.WithNodesMatch(nodes, match));
            }
        }

        private static void CollectDeliveries(ClusterState state, ExploreProfile profile, BrokenMode broken, List<ClusterState> output)
        {
            for (var mi = 0; mi < state.Messages.Count; mi++)
            {
                var msg = state.Messages[mi];
                if (!state.CanCommunicate(msg.From, msg.To))
                    continue;

                var delivered = ModelRpcTransitions.DeliverOne(state, mi, profile, broken);
                output.Add(delivered);
            }
        }

        private static void CollectDrops(ClusterState state, List<ClusterState> output)
        {
            for (var i = 0; i < state.Messages.Count; i++)
            {
                var messages = ModelTransitionUtil.CloneMessages(state.Messages);
                messages.RemoveAt(i);
                output.Add(state.WithMessages(messages));
            }
        }

        private static void CollectDuplicates(ClusterState state, ExploreProfile profile, List<ClusterState> output)
        {
            if (state.Messages.Count == 0 || state.Messages.Count >= profile.MaxInFlight)
                return;

            for (var i = 0; i < state.Messages.Count; i++)
            {
                var messages = ModelTransitionUtil.CloneMessages(state.Messages);
                var src = state.Messages[i];
                messages.Add(
                    new InFlightMessage(
                        state.NextMessageId,
                        new MessagePayload(
                            new MessageRoute(src.Kind, src.From, src.To, src.Term),
                            new MessageExtras(src.LastLogIndex, src.LastLogTerm, src.Success, src.MatchIndex, src.ReadIndex))));
                output.Add(state.WithMessages(messages, state.NextMessageId + 1));
            }
        }

        private static void CollectElections(ClusterState state, ExploreProfile profile, List<ClusterState> output)
        {
            for (var i = 0; i < state.Nodes.Count; i++)
            {
                if (!TryBuildElection(state, profile, i, out var next))
                    continue;

                output.Add(next);
            }
        }

        private static void CollectPartitions(ClusterState state, List<ClusterState> output)
        {
            if (state.Nodes.Count < 2)
                return;

            // M8-01 bound: only single-node isolation and full heal (see ADR search bounds).
            var parts = state.Partitions;
            var allSame = true;
            for (var i = 1; i < parts.Count; i++)
            {
                if (parts[i] == parts[0])
                    continue;

                allSame = false;
                break;
            }

            if (allSame)
            {
                AddIsolationPartitions(state, parts, output);
                return;
            }

            AddHealedPartition(state, parts, output);
        }

        private static void CollectReadIndexes(ClusterState state, ExploreProfile profile, BrokenMode broken, List<ClusterState> output)
        {
            if (profile.MaxPendingReads <= 0)
                return;

            for (var i = 0; i < state.Nodes.Count; i++)
            {
                if (!TryBuildReadIndex(state, profile, broken, i, out var next))
                    continue;

                output.Add(next);
            }
        }

        /// <summary>Decides whether a node may serve its pending read index.</summary>
        /// <param name="node">The node attempting the read.</param>
        /// <param name="applied">The leader-side applied index the read is checked against.</param>
        /// <param name="profile">The exploration profile carrying the majority size.</param>
        /// <param name="broken">The injected fault mode, if any.</param>
        /// <remarks>
        /// A read is ready with a pending index, a majority of read acks, and the index applied locally.
        /// Under the injected <see cref="BrokenMode.ReadIndex" /> fault the majority/apply wait is skipped,
        /// so the safety checker is expected to report the resulting violation.
        /// </remarks>
        private static bool ComputeReadReady(NodeState node, int applied, ExploreProfile profile, BrokenMode broken)
        {
            var hasPendingRead = node.ReadIndex > 0;
            var majorityConfirmed = VoteMask.CountGranted(node.ReadAcks) >= profile.Majority && applied >= node.ReadIndex;
            var quorumSkipped = broken is BrokenMode.ReadIndex; // injected ReadIndex fault skips the quorum wait; the safety checker must catch the violation.
            return hasPendingRead && (quorumSkipped || majorityConfirmed);
        }

        private static bool TryBuildClientProposal(ClusterState state, ExploreProfile profile, int leaderId, out ClusterState after)
        {
            after = state;
            var leader = state.Nodes[leaderId];
            if (leader.Role != NodeRole.Leader)
                return false;

            if (leader.LogEntries.Count >= profile.MaxLogEntries)
                return false;

            if (state.Messages.Count + (state.Nodes.Count - 1) > profile.MaxInFlight)
                return false;

            var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
            var newIndex = leader.LastLogIndex + 1;
            var log = ModelTransitionUtil.AppendEntry(leader.LogEntries, new LogEntry(leader.CurrentTerm, newIndex));
            nodes[leaderId] = ModelTransitionUtil.Patch(leader, new NodePatch { LogEntries = log });

            var match = ModelTransitionUtil.CloneInts(state.MatchIndexes);
            match[leaderId] = newIndex;

            var messages = ModelTransitionUtil.CloneMessages(state.Messages);
            var nextId = ModelTransitionUtil.EnqueueAppendEntries(state, leaderId, leader.CurrentTerm, newIndex, leader.CurrentTerm, messages, state.NextMessageId);

            after = ModelRpcCommit.MaybeAdvanceCommit(state.WithNodesMessagesMatch(nodes, messages, nextId, match), leaderId, profile, BrokenMode.None);
            return true;
        }

        private static bool TryBuildElection(ClusterState state, ExploreProfile profile, int candidateId, out ClusterState next)
        {
            next = state;
            var node = state.Nodes[candidateId];
            if (node.Role is NodeRole.Leader)
                return false;

            var nextTerm = node.CurrentTerm + 1;
            if (nextTerm > profile.MaxTerm)
                return false;

            if (state.Messages.Count + (state.Nodes.Count - 1) > profile.MaxInFlight)
                return false;

            var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
            var nodePatch = new NodePatch
            {
                Role = NodeRole.Candidate,
                CurrentTerm = nextTerm,
                VotedFor = candidateId,
                VotesGranted = 1 << candidateId,
                ReadIndex = 0,
                ReadAcks = 0,
                ReadReady = false,
            };
            nodes[candidateId] = ModelTransitionUtil.Patch(node, nodePatch);

            var messages = ModelTransitionUtil.CloneMessages(state.Messages);
            var nextId = ModelTransitionUtil.EnqueueVoteRequests(state, candidateId, nextTerm, nodes[candidateId], messages, state.NextMessageId);

            // Keep cluster MatchIndex: wiping it on candidacy would erase a live older-term leader's
            // replication progress. BecomeLeader resets MatchIndex when a candidate wins.
            next = state.WithNodesMessages(nodes, messages, nextId);
            return true;
        }

        private static bool TryBuildReadIndex(ClusterState state, ExploreProfile profile, BrokenMode broken, int leaderId, out ClusterState next)
        {
            next = state;
            var leader = state.Nodes[leaderId];
            if (leader.Role != NodeRole.Leader || leader.ReadIndex > 0)
                return false;

            if (state.Messages.Count + (state.Nodes.Count - 1) > profile.MaxInFlight)
                return false;

            var readIndex = Math.Max(leader.CommitIndex, 1);
            var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
            nodes[leaderId] = ModelTransitionUtil.Patch(leader, new NodePatch { ReadIndex = readIndex, ReadAcks = 1 << leaderId, ReadReady = false });

            if (broken is BrokenMode.ReadIndex)
            {
                // Broken: serve immediately without majority confirm / apply wait.
                nodes[leaderId] = ModelTransitionUtil.Patch(nodes[leaderId], new NodePatch { ReadReady = true });
                next = state.WithNodes(nodes);
                return true;
            }

            var messages = ModelTransitionUtil.CloneMessages(state.Messages);
            var nextId = ModelTransitionUtil.EnqueueReadIndexRequests(state, leaderId, leader.CurrentTerm, readIndex, messages, state.NextMessageId);
            next = state.WithNodesMessages(nodes, messages, nextId);
            return true;
        }

        private static bool TryFindOldestOldTermEntry(NodeState leader, out LogEntry old)
        {
            for (var j = 0; j < leader.LogEntries.Count; j++)
            {
                var entry = leader.LogEntries[j];
                if (entry.Index <= leader.CommitIndex || entry.Term >= leader.CurrentTerm)
                    continue;

                old = entry;
                return true;
            }

            old = default;
            return false;
        }

        private static class ModelRpcCommit
        {
            internal static AppendOutcome ApplyAppendEntries(InFlightMessage msg, NodeState[] nodes, ExploreProfile profile)
            {
                var receiver = nodes[msg.To];
                nodes[msg.To] = ModelTransitionUtil.Patch(receiver, new NodePatch { Role = NodeRole.Follower, VotesGranted = 0, ReadIndex = 0, ReadAcks = 0, ReadReady = false });
                receiver = nodes[msg.To];

                // The committed prefix is immutable: only an entry with a matching term may be confirmed.
                var touchesCommittedPrefix = msg.LastLogIndex <= receiver.CommitIndex;
                if (touchesCommittedPrefix)
                {
                    var existing = ModelTransitionUtil.FindEntry(receiver.LogEntries, msg.LastLogIndex);
                    var termMatches = existing != null && existing.Value.Term == msg.LastLogTerm;
                    var matchIndex = termMatches ? msg.LastLogIndex : receiver.LastLogIndex;
                    return new AppendOutcome(termMatches, matchIndex);
                }

                // The uncommitted tail is bounded by the profile; anything beyond it is rejected.
                var beyondLogBound = msg.LastLogIndex > profile.MaxLogEntries;
                return beyondLogBound ? new AppendOutcome(false, receiver.LastLogIndex) : AcceptUncommittedAppend(msg, nodes, receiver);
            }

            internal static ClusterState BecomeLeader(ClusterState state, int leaderId, TransitionScratch scratch, ExploreProfile profile, int votes)
            {
                var nodes = scratch.Nodes;
                var messages = scratch.Messages;
                var match = scratch.Match;
                var nextId = scratch.NextMessageId;
                var candidate = nodes[leaderId];
                if (candidate.LogEntries.Count >= profile.MaxLogEntries)
                {
                    nodes[leaderId] = ModelTransitionUtil.Patch(candidate, new NodePatch { Role = NodeRole.Leader, VotesGranted = votes });
                    for (var i = 0; i < match.Length; i++)
                        match[i] = i == leaderId ? nodes[leaderId].LastLogIndex : 0;

                    return state.WithNodesMessagesMatch(nodes, messages, nextId, match);
                }

                var noopIndex = candidate.LastLogIndex + 1;
                var log = ModelTransitionUtil.AppendEntry(candidate.LogEntries, new LogEntry(candidate.CurrentTerm, noopIndex));
                nodes[leaderId] = ModelTransitionUtil.Patch(candidate, new NodePatch { Role = NodeRole.Leader, LogEntries = log, VotesGranted = votes });
                for (var i = 0; i < match.Length; i++)
                    match[i] = i == leaderId ? noopIndex : 0;

                if (messages.Count + (state.Nodes.Count - 1) <= profile.MaxInFlight)
                    nextId = ModelTransitionUtil.EnqueueAppendEntries(state, leaderId, candidate.CurrentTerm, noopIndex, candidate.CurrentTerm, messages, nextId);

                scratch.NextMessageId = nextId;
                var become = state.WithNodesMessagesMatch(nodes, messages, nextId, match);
                return MaybeAdvanceCommit(become, leaderId, profile, BrokenMode.None);
            }

            internal static NodeState DemoteFollower(NodeState node, int term) => ModelTransitionUtil.Patch(
                node,
                new NodePatch
                {
                    Role = NodeRole.Follower,
                    CurrentTerm = term,
                    VotedFor = -1,
                    VotesGranted = 0,
                    ReadIndex = 0,
                    ReadAcks = 0,
                    ReadReady = false,
                });

            internal static ClusterState MaybeAdvanceCommit(ClusterState state, int leaderId, ExploreProfile profile, BrokenMode broken)
            {
                var leader = state.Nodes[leaderId];
                if (leader.Role != NodeRole.Leader)
                    return state;

                var match = ModelTransitionUtil.CloneInts(state.MatchIndexes);
                match[leaderId] = leader.LastLogIndex;

                var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
                if (!TryFindNewCommit(new CommitProbe(leader, nodes, match, profile, broken), out var newCommit, out var badOld))
                    return state.WithMatchIndexes(match);

                nodes[leaderId] = ModelTransitionUtil.Patch(leader, new NodePatch { CommitIndex = newCommit, BadOldCommit = badOld || leader.BadOldCommit });
                PropagateFollowerCommits(nodes, match, leaderId, newCommit);
                return state.WithNodesMatch(nodes, match);
            }

            internal static bool TryGrantVote(InFlightMessage msg, NodeState[] nodes, BrokenMode broken, NodeState receiver)
            {
                var canVote = receiver.VotedFor == -1 || receiver.VotedFor == msg.From || broken is BrokenMode.Vote;

                if (!canVote || !IsLogUpToDate(msg.LastLogTerm, msg.LastLogIndex, receiver))
                    return false;

                nodes[msg.To] = ModelTransitionUtil.Patch(
                    receiver,
                    new NodePatch { Role = NodeRole.Follower, VotedFor = msg.From, VotesGranted = 0, ReadIndex = 0, ReadAcks = 0, ReadReady = false });
                return true;
            }

            private static AppendOutcome AcceptUncommittedAppend(InFlightMessage msg, NodeState[] nodes, NodeState receiver)
            {
                // Truncate only the uncommitted suffix, then append the offered entry.
                // Never truncate at or below CommitIndex (committed prefix is immutable).
                var truncateTo = msg.LastLogIndex - 1;
                if (truncateTo < receiver.CommitIndex)
                    return new AppendOutcome(false, receiver.LastLogIndex);

                var prefix = ModelTransitionUtil.FindEntry(receiver.LogEntries, truncateTo);
                var prefixOk = msg.LastLogIndex == 1 || prefix != null;
                if (!prefixOk)
                    return new AppendOutcome(false, receiver.LastLogIndex);

                var log = ModelTransitionUtil.TruncateTo(receiver.LogEntries, truncateTo);
                log = ModelTransitionUtil.AppendEntry(log, new LogEntry(msg.LastLogTerm, msg.LastLogIndex));
                nodes[msg.To] = ModelTransitionUtil.Patch(receiver, new NodePatch { LogEntries = log });
                return new AppendOutcome(true, msg.LastLogIndex);
            }

            private static bool CanPropagateCommit(NodeState node, int[] match, int nodeIndex, LogEntry leaderEntry, int newCommit)
            {
                if (match[nodeIndex] < newCommit || node.CommitIndex >= newCommit)
                    return false;

                var stored = ModelTransitionUtil.FindEntry(node.LogEntries, newCommit);
                return stored != null && stored.Value.Term == leaderEntry.Term;
            }

            private static bool HasCurrentTermEntryThrough(NodeState leader, int index)
            {
                for (var i = 0; i < leader.LogEntries.Count; i++)
                {
                    if (leader.LogEntries[i].Term == leader.CurrentTerm && leader.LogEntries[i].Index <= index)
                        return true;
                }

                return false;
            }

            private static bool HasMatchingMajority(NodeState leader, IReadOnlyList<NodeState> nodes, int[] match, int index, int majority)
            {
                // MatchIndex is cluster-shared; a stale leader must not count another leader's
                // replication progress. Only replicas that store the leader's entry at index count.
                var leaderEntry = ModelTransitionUtil.FindEntry(leader.LogEntries, index);
                if (leaderEntry == null)
                    return false;

                var replicas = 0;
                for (var i = 0; i < nodes.Count; i++)
                {
                    if (match[i] >= index && StoresMatchingTerm(nodes[i], index, leaderEntry.Value.Term))
                        replicas++;
                }

                return replicas >= majority;
            }

            private static bool IsContiguousMatchingMajority(NodeState leader, IReadOnlyList<NodeState> nodes, int[] match, int fromInclusive, int toInclusive, int majority)
            {
                for (var idx = fromInclusive; idx <= toInclusive; idx++)
                {
                    if (!HasMatchingMajority(leader, nodes, match, idx, majority))
                        return false;
                }

                return true;
            }

            private static bool IsLogUpToDate(int candidateLastTerm, int candidateLastIndex, NodeState voter) =>
                candidateLastTerm != voter.LastLogTerm ? candidateLastTerm > voter.LastLogTerm : candidateLastIndex >= voter.LastLogIndex;

            private static void PropagateFollowerCommits(NodeState[] nodes, int[] match, int leaderId, int newCommit)
            {
                var leaderEntry = ModelTransitionUtil.FindEntry(nodes[leaderId].LogEntries, newCommit);
                if (leaderEntry == null)
                    return;

                for (var i = 0; i < nodes.Length; i++)
                {
                    if (i == leaderId || !CanPropagateCommit(nodes[i], match, i, leaderEntry.Value, newCommit))
                        continue;

                    nodes[i] = ModelTransitionUtil.Patch(nodes[i], new NodePatch { CommitIndex = Math.Min(newCommit, nodes[i].LastLogIndex) });
                }
            }

            private static bool StoresMatchingTerm(NodeState node, int index, int term)
            {
                var stored = ModelTransitionUtil.FindEntry(node.LogEntries, index);
                return stored != null && stored.Value.Term == term;
            }

            private static bool TryClassifyCommitCandidate(CommitProbe probe, int index, out bool badOld)
            {
                badOld = false;
                if (!HasMatchingMajority(probe.Leader, probe.Nodes, probe.Match, index, probe.Profile.Majority))
                    return false;

                var entry = ModelTransitionUtil.FindEntry(probe.Leader.LogEntries, index);
                if (entry == null)
                    return false;

                if (HasCurrentTermEntryThrough(probe.Leader, index))
                    return IsContiguousMatchingMajority(probe.Leader, probe.Nodes, probe.Match, probe.Leader.CommitIndex + 1, index, probe.Profile.Majority);

                if (probe.Broken != BrokenMode.CurrentTermCommit)
                    return false;

                badOld = true;
                return IsContiguousMatchingMajority(probe.Leader, probe.Nodes, probe.Match, probe.Leader.CommitIndex + 1, index, probe.Profile.Majority);
            }

            private static bool TryFindNewCommit(CommitProbe probe, out int newCommit, out bool badOld)
            {
                for (var n = probe.Leader.LastLogIndex; n > probe.Leader.CommitIndex; n--)
                {
                    if (!TryClassifyCommitCandidate(probe, n, out var candidateIsBadOld))
                        continue;

                    newCommit = n;
                    badOld = candidateIsBadOld;
                    return true;
                }

                newCommit = probe.Leader.CommitIndex;
                badOld = false;
                return false;
            }
        }

        private static class ModelRpcTransitions
        {
            internal static ClusterState DeliverOne(ClusterState state, int messageIndex, ExploreProfile profile, BrokenMode broken)
            {
                var msg = state.Messages[messageIndex];
                var messages = ModelTransitionUtil.CloneMessages(state.Messages);
                messages.RemoveAt(messageIndex);

                var nodes = ModelTransitionUtil.CloneNodes(state.Nodes);
                var match = ModelTransitionUtil.CloneInts(state.MatchIndexes);
                var nextId = state.NextMessageId;

                return msg.Kind switch
                {
                    MsgKind.RequestVote => HandleRequestVote(state, msg, new TransitionScratch(nodes, messages, match, nextId), broken, profile),
                    MsgKind.VoteResponse => HandleVoteResponse(state, msg, new TransitionScratch(nodes, messages, match, nextId), profile),
                    MsgKind.AppendEntries => HandleAppendEntries(state, msg, new TransitionScratch(nodes, messages, match, nextId), profile),
                    MsgKind.AppendResponse => HandleAppendResponse(state, msg, new TransitionScratch(nodes, messages, match, nextId), profile, broken),
                    MsgKind.ReadIndexRequest => HandleReadIndexRequest(state, msg, new TransitionScratch(nodes, messages, match, nextId), profile),
                    MsgKind.ReadIndexResponse => HandleReadIndexResponse(state, msg, new TransitionScratch(nodes, messages, match, nextId), profile, broken),
                    _ => throw new ArgumentOutOfRangeException(nameof(state), msg.Kind, "Unsupported message kind."),
                };
            }

            private static bool ComputeReadResponseReady(NodeState leader, int acks, ExploreProfile profile, BrokenMode broken) =>
                (VoteMask.CountGranted(acks) >= profile.Majority && leader.AppliedIndex >= leader.ReadIndex) || broken is BrokenMode.ReadIndex;

            private static ClusterState HandleAppendEntries(ClusterState state, InFlightMessage msg, TransitionScratch scratch, ExploreProfile profile)
            {
                var nodes = scratch.Nodes;
                _ = scratch.Messages;
                _ = scratch.Match;
                _ = scratch.NextMessageId;
                var receiver = RefreshAppendReceiver(nodes, msg);
                var applied = ApplyAppendIfCurrent(msg, nodes, receiver, profile);
                return EnqueueAppendResponse(state, msg, scratch, profile, applied);
            }

            private static NodeState RefreshAppendReceiver(NodeState[] nodes, InFlightMessage msg)
            {
                var receiver = nodes[msg.To];
                if (msg.Term <= receiver.CurrentTerm)
                    return receiver;

                nodes[msg.To] = ModelRpcCommit.DemoteFollower(receiver, msg.Term);
                return nodes[msg.To];
            }

            private static AppendOutcome ApplyAppendIfCurrent(InFlightMessage msg, NodeState[] nodes, NodeState receiver, ExploreProfile profile)
            {
                if (msg.Term < receiver.CurrentTerm)
                    return new AppendOutcome(false, receiver.LastLogIndex);

                var outcome = ModelRpcCommit.ApplyAppendEntries(msg, nodes, profile);
                return new AppendOutcome(outcome.Success, outcome.MatchIndex);
            }

            private static ClusterState EnqueueAppendResponse(ClusterState state, InFlightMessage msg, TransitionScratch scratch, ExploreProfile profile, AppendOutcome applied)
            {
                var nodes = scratch.Nodes;
                var messages = scratch.Messages;
                var match = scratch.Match;
                var nextId = scratch.NextMessageId;
                var responseFrom = msg.To;
                var responseTo = msg.From;
                if (messages.Count >= profile.MaxInFlight || !state.CanCommunicate(responseFrom, responseTo))
                    return state.WithNodesMessagesMatch(nodes, messages, nextId, match);
                var payload = MessagePayload.AppendResponse(responseFrom, responseTo, nodes[msg.To].CurrentTerm, msg.LastLogIndex, msg.LastLogTerm, applied.Success, applied.MatchIndex);
                messages.Add(new InFlightMessage(nextId++, payload));
                return state.WithNodesMessagesMatch(nodes, messages, nextId, match);
            }

            private static ClusterState HandleAppendResponse(ClusterState state, InFlightMessage msg, TransitionScratch scratch, ExploreProfile profile, BrokenMode broken)
            {
                var nodes = scratch.Nodes;
                var messages = scratch.Messages;
                var match = scratch.Match;
                var leader = nodes[msg.To];
                if (leader.Role != NodeRole.Leader)
                    return state.WithNodesMessagesMatch(nodes, messages, match);

                if (msg.Term > leader.CurrentTerm)
                {
                    nodes[msg.To] = ModelRpcCommit.DemoteFollower(leader, msg.Term);
                    return state.WithNodesMessagesMatch(nodes, messages, match);
                }

                if (msg.Term != leader.CurrentTerm || !msg.Success)
                    return state.WithNodesMessagesMatch(nodes, messages, match);

                if (msg.MatchIndex > match[msg.From])
                    match[msg.From] = msg.MatchIndex;

                match[msg.To] = leader.LastLogIndex;
                var after = state.WithNodesMessagesMatch(nodes, messages, match);
                return ModelRpcCommit.MaybeAdvanceCommit(after, msg.To, profile, broken);
            }

            private static ClusterState HandleReadIndexRequest(ClusterState state, InFlightMessage msg, TransitionScratch scratch, ExploreProfile profile)
            {
                var nodes = scratch.Nodes;
                var messages = scratch.Messages;
                var match = scratch.Match;
                var nextId = scratch.NextMessageId;
                var receiver = nodes[msg.To];
                var ok = msg.Term >= receiver.CurrentTerm;
                if (msg.Term > receiver.CurrentTerm)
                    nodes[msg.To] = ModelRpcCommit.DemoteFollower(receiver, msg.Term);

                // Response travels receiver → original sender (swap relative to request From/To).
                var responseFrom = msg.To;
                var responseTo = msg.From;
                if (messages.Count >= profile.MaxInFlight || !state.CanCommunicate(responseFrom, responseTo))
                    return state.WithNodesMessagesMatch(nodes, messages, nextId, match);
                var payload = MessagePayload.ReadResponse(responseFrom, responseTo, Math.Max(msg.Term, nodes[msg.To].CurrentTerm), ok, msg.ReadIndex);
                messages.Add(new InFlightMessage(nextId++, payload));
                return state.WithNodesMessagesMatch(nodes, messages, nextId, match);
            }

            private static ClusterState HandleReadIndexResponse(ClusterState state, InFlightMessage msg, TransitionScratch scratch, ExploreProfile profile, BrokenMode broken)
            {
                var nodes = scratch.Nodes;
                var messages = scratch.Messages;
                var match = scratch.Match;
                var leader = nodes[msg.To];
                if (IsStaleReadResponse(leader, msg))
                    return state.WithNodesMessagesMatch(nodes, messages, match);

                var acks = leader.ReadAcks | (1 << msg.From);
                var ready = ComputeReadResponseReady(leader, acks, profile, broken);

                nodes[msg.To] = ModelTransitionUtil.Patch(leader, new NodePatch { ReadAcks = acks, ReadReady = ready });
                return state.WithNodesMessagesMatch(nodes, messages, match);
            }

            private static ClusterState HandleRequestVote(ClusterState state, InFlightMessage msg, TransitionScratch scratch, BrokenMode broken, ExploreProfile profile)
            {
                var nodes = scratch.Nodes;
                var messages = scratch.Messages;
                var match = scratch.Match;
                var nextId = scratch.NextMessageId;
                var receiver = nodes[msg.To];
                if (msg.Term > receiver.CurrentTerm)
                {
                    nodes[msg.To] = ModelRpcCommit.DemoteFollower(receiver, msg.Term);
                    receiver = nodes[msg.To];
                }

                var grant = false;
                if (msg.Term == receiver.CurrentTerm)
                    grant = ModelRpcCommit.TryGrantVote(msg, nodes, broken, receiver);

                // Response travels receiver → original sender (swap relative to request From/To).
                var responseFrom = msg.To;
                var responseTo = msg.From;
                if (messages.Count < profile.MaxInFlight && state.CanCommunicate(responseFrom, responseTo))
                    messages.Add(new InFlightMessage(nextId++, MessagePayload.VoteResponse(responseFrom, responseTo, Math.Max(msg.Term, nodes[msg.To].CurrentTerm), grant)));

                scratch.NextMessageId = nextId;
                return state.WithNodesMessagesMatch(nodes, messages, nextId, match);
            }

            private static ClusterState HandleVoteResponse(ClusterState state, InFlightMessage msg, TransitionScratch scratch, ExploreProfile profile)
            {
                var nodes = scratch.Nodes;
                var messages = scratch.Messages;
                var match = scratch.Match;
                var nextId = scratch.NextMessageId;
                var candidate = nodes[msg.To];
                if (msg.Term > candidate.CurrentTerm)
                {
                    nodes[msg.To] = ModelRpcCommit.DemoteFollower(candidate, msg.Term);
                    return state.WithNodesMessagesMatch(nodes, messages, nextId, match);
                }

                if (candidate.Role != NodeRole.Candidate || msg.Term != candidate.CurrentTerm || !msg.Success)
                    return state.WithNodesMessagesMatch(nodes, messages, nextId, match);

                var votes = candidate.VotesGranted | (1 << msg.From);
                nodes[msg.To] = ModelTransitionUtil.Patch(candidate, new NodePatch { VotesGranted = votes });

                // A candidate becomes leader once its votes reach a majority; otherwise the vote is only recorded.
                var majorityReached = VoteMask.CountGranted(votes) >= profile.Majority;
                return majorityReached ? Promote() : state.WithNodesMessagesMatch(nodes, messages, nextId, match);

                ClusterState Promote()
                {
                    return ModelRpcCommit.BecomeLeader(state, msg.To, new TransitionScratch(nodes, messages, match, nextId), profile, votes);
                }
            }

            private static bool IsStaleReadResponse(NodeState leader, InFlightMessage msg) => leader.Role != NodeRole.Leader || leader.ReadIndex == 0 ||
                                                                                              msg.Term != leader.CurrentTerm || !msg.Success || msg.ReadIndex != leader.ReadIndex;

            [Immutable]
            private sealed record AppendOutcome(bool Success, int MatchIndex);
        }
    }

    private static class StateExplorer
    {
        internal static ExploreResult Explore(ExploreProfile profile, BrokenMode broken)
        {
            var initial = ClusterState.CreateInitial(profile.ReplicaCount);
            var work = new SearchWork(profile, broken);
            var startFp = initial.Fingerprint(profile.SymmetryReduce);
            _ = work.Seen.Add(startFp);
            work.Parents[startFp] = null;
            work.Queue.Enqueue(initial);

            var initialViolation = SafetyChecker.Check(initial);
            return initialViolation != null ? new ExploreResult(1, 0, initialViolation, initial, true, ReconstructPath(work.Parents, startFp)) : Search(work);
        }

        private static string[] ReconstructPath(Dictionary<string, string?> parents, string endFingerprint)
        {
            var stack = new Stack<string>();
            var current = endFingerprint;
            while (current != null)
            {
                stack.Push(current);
                if (!parents.TryGetValue(current, out current))
                    break;
            }

            var path = new string[stack.Count];
            for (var i = 0; i < path.Length; i++)
                path[i] = stack.Pop();

            return path;
        }

        private static ExploreResult Search(SearchWork work)
        {
            var transitions = 0;
            while (work.Queue.Count > 0 && work.Seen.Count < work.Profile.MaxStates)
            {
                var current = work.Queue.Dequeue();
                var currentFp = current.Fingerprint(work.Profile.SymmetryReduce);
                ModelTransitions.CollectSuccessors(current, work.Profile, work.Broken, work.Successors);
                for (var i = 0; i < work.Successors.Count; i++)
                {
                    transitions++;
                    var outcome = VisitSuccessor(work, currentFp, work.Successors[i], transitions);
                    if (outcome != null)
                        return outcome;
                }
            }

            return new ExploreResult(work.Seen.Count, transitions, null, null, work.Queue.Count == 0, null);
        }

        private static ExploreResult? VisitSuccessor(SearchWork work, string currentFp, ClusterState next, int transitions)
        {
            var fp = next.Fingerprint(work.Profile.SymmetryReduce);
            if (!work.Seen.Add(fp))
                return null;

            work.Parents[fp] = currentFp;
            var violation = SafetyChecker.Check(next);
            if (violation != null)
                return new ExploreResult(work.Seen.Count, transitions, violation, next, false, ReconstructPath(work.Parents, fp));

            if (work.Seen.Count >= work.Profile.MaxStates)
                return new ExploreResult(work.Seen.Count, transitions, null, null, false, null);

            work.Queue.Enqueue(next);
            return null;
        }

        [Immutable]
        private sealed class SearchWork
        {
            internal SearchWork(ExploreProfile profile, BrokenMode broken)
            {
                Profile = profile;
                Broken = broken;
                Seen = new HashSet<string>(StringComparer.Ordinal);
                Parents = new Dictionary<string, string?>(StringComparer.Ordinal);
                Queue = new Queue<ClusterState>();
                Successors = new List<ClusterState>(64);
            }

            internal BrokenMode Broken { get; }

            internal Dictionary<string, string?> Parents { get; }

            internal ExploreProfile Profile { get; }

            internal Queue<ClusterState> Queue { get; }

            internal HashSet<string> Seen { get; }

            internal List<ClusterState> Successors { get; }
        }
    }

    [Immutable]
    private sealed class CommitProbe
    {
        internal CommitProbe(NodeState leader, IReadOnlyList<NodeState> nodes, int[] match, ExploreProfile profile, BrokenMode broken)
        {
            Leader = leader;
            Nodes = nodes;
            Match = match;
            Profile = profile;
            Broken = broken;
        }

        internal BrokenMode Broken { get; }

        internal NodeState Leader { get; }

        internal int[] Match { get; }

        internal IReadOnlyList<NodeState> Nodes { get; }

        internal ExploreProfile Profile { get; }
    }

    [Immutable]
    private sealed class TraceSearchWork
    {
        internal TraceSearchWork(IReadOnlyList<ModelCommitTracePoint> tracePoints, int maxStates)
        {
            TracePoints = tracePoints;
            MaxStates = maxStates;
            Seen = new HashSet<string>(StringComparer.Ordinal);
            Queue = new Queue<TraceSearchState>();
            Successors = new List<ClusterState>(64);
        }

        internal int MaxStates { get; }

        internal Queue<TraceSearchState> Queue { get; }

        internal HashSet<string> Seen { get; }

        internal List<ClusterState> Successors { get; }

        internal IReadOnlyList<ModelCommitTracePoint> TracePoints { get; }
    }
}
