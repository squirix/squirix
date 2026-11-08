using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

internal static class ExploreCliRunner
{
    private const int ExitBrokenWithoutViolation = 3;
    private const int ExitBudgetExhausted = 4;
    private const int ExitPolicyViolation = 5;
    private const int ExitSuccess = 0;
    private const int ExitViolation = 2;

    internal static async Task<int> RunCliAsync(string profileName, string outputDir, BrokenMode broken)
    {
        _ = Directory.CreateDirectory(outputDir);
        var results = CollectResults(profileName, broken);
        var aggregate = AggregateResults(results);
        var summary = new SummaryContent(profileName, broken, aggregate.TotalStates, aggregate.TotalTransitions, aggregate.FirstViolation, aggregate.FirstPolicy, aggregate.AllFixedPoint);
        await WriteSummaryAsync(outputDir, in summary, CancellationToken.None).ConfigureAwait(false);

        if (aggregate.FirstPolicy != null && aggregate.FirstPolicyState != null)
        {
            var policyFileName = ProtocolPolicyChecker.PolicyReportFileName(aggregate.FirstPolicy.Policy);
            RemoveObsoletePolicyReports(outputDir, policyFileName);
            var policyJson = ProtocolPolicyChecker.FormatCounterexampleJson(aggregate.FirstPolicy, aggregate.FirstPolicyState, FindPolicyPath(results, aggregate.FirstPolicy));
            await File.WriteAllTextAsync(Path.Join(outputDir, policyFileName), policyJson, Encoding.UTF8, CancellationToken.None).ConfigureAwait(false);
        }
        else
        {
            RemoveObsoletePolicyReports(outputDir, null);
        }

        if (aggregate.FirstViolation == null || aggregate.FirstState == null)
        {
            var staleCounterexample = Path.Join(outputDir, "counterexample.json");
            if (File.Exists(staleCounterexample))
                File.Delete(staleCounterexample);

            return ExitCode(broken, aggregate.FirstViolation, aggregate.FirstPolicy, aggregate.AllFixedPoint);
        }

        var path = FindPath(results, aggregate.FirstViolation);
        var json = SafetyChecker.FormatCounterexampleJson(aggregate.FirstViolation, aggregate.FirstState, path);
        await File.WriteAllTextAsync(Path.Join(outputDir, "counterexample.json"), json, Encoding.UTF8, CancellationToken.None).ConfigureAwait(false);

        return ExitCode(broken, aggregate.FirstViolation, aggregate.FirstPolicy, aggregate.AllFixedPoint);
    }

    /// <summary>Removes the generated policy reports that the current run did not produce.</summary>
    /// <param name="outputDir">The output directory, which a caller may reuse across runs.</param>
    /// <param name="keepFileName">The report this run writes, or <see langword="null" /> when it writes none.</param>
    /// <remarks>
    /// The names removed here come from <see cref="ProtocolPolicyChecker.PolicyNames" />, so the set this method may delete is
    /// exactly the set the CLI can write. The name of the written report is chosen per policy, so deleting a single fixed name
    /// left the previous run's report in place while the summary beside it said <c lang="json">"policyViolation":null</c> — an
    /// artifact set describing a run that did not happen. Matching <c lang="text">policy-*.json</c> instead also matched files
    /// this program never writes: a caller's own <c lang="text">policy-user-notes.json</c> was deleted by a run that produced
    /// no report at all. Only names this checker can report are ours; anything else in the directory belongs to the caller.
    /// </remarks>
    private static void RemoveObsoletePolicyReports(string outputDir, string? keepFileName)
    {
        var names = ProtocolPolicyChecker.PolicyNames;
        for (var i = 0; i < names.Count; i++)
        {
            var generated = ProtocolPolicyChecker.PolicyReportFileName(names[i]);
            if (string.Equals(generated, keepFileName, StringComparison.Ordinal))
                continue;

            var path = Path.Join(outputDir, generated);
            if (File.Exists(path))
                File.Delete(path);
        }
    }

    private static ResultAggregate AggregateResults(List<(ExploreProfile Profile, ExploreResult Result)> results)
    {
        SafetyViolation? firstViolation = null;
        ClusterState? firstState = null;
        var (firstPolicy, firstPolicyState) = FindFirstPolicy(results);
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

        return new ResultAggregate(totalStates, totalTransitions, firstViolation, firstState, firstPolicy, firstPolicyState, allFixedPoint);
    }

    private static (PolicyViolation? Policy, ClusterState? State) FindFirstPolicy(List<(ExploreProfile Profile, ExploreResult Result)> results)
    {
        for (var i = 0; i < results.Count; i++)
        {
            var result = results[i].Result;
            if (result.PolicyViolation != null)
                return (result.PolicyViolation, result.PolicyViolatingState);
        }

        return (null, null);
    }

    private static void CollectFullProfiles(BrokenMode broken, List<(ExploreProfile Profile, ExploreResult Result)> results)
    {
        // Elections RF=2..5 up to 3 terms; commit/read bounds from the milestone profile.
        for (var rf = 2; rf <= 5; rf++)
        {
            var election = ExploreProfile.ForReplicaCount(rf, 3, 1, 4, 0, true, true);
            results.Add((election, ExploreRunner.Run(election, broken)));

            var commit = ExploreProfile.ForReplicaCount(rf, 3, 3, 4, 0, true, true);
            results.Add((commit, ExploreRunner.Run(commit, broken)));

            var read = ExploreProfile.ForReplicaCount(rf, 2, 2, 4, 1, false, true);
            results.Add((read, ExploreRunner.Run(read, broken)));
        }

        // One member loses its disk and a leader repairs it. RF=3 is explored to its fixed point; RF=5 needs four messages in flight for
        // one election and exceeds any affordable state budget before the repair without a ceiling shows, so it is left out of the bounds.
        var diskLoss = ExploreProfile.SmallDiskLoss();
        results.Add((diskLoss, ExploreRunner.Run(diskLoss, broken)));

        // The log rules with one-step elections and catch-up: four terms are what an earlier-term entry needs to be lost to a later leader.
        var logRules = ExploreProfile.EarlierTermLogRules();
        results.Add((logRules, ExploreRunner.Run(logRules, broken)));
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
        results.Add((profile, ExploreRunner.Run(profile, broken)));
        return results;
    }

    private static int ExitCode(BrokenMode broken, SafetyViolation? firstViolation, PolicyViolation? firstPolicy, bool allFixedPoint)
    {
        // Success = clean exhaustion or the injected break manifesting; 2 = data-safety violation; 3 = injected break stayed
        // silent; 4 = hit documented MaxStates (residual risk, not a counterexample); 5 = a step the protocol does not allow,
        // which is not a data loss and is therefore its own code.
        var found = firstViolation != null || firstPolicy != null;
        return (broken != BrokenMode.None, found, firstViolation != null, allFixedPoint) switch
        {
            (true, true, _, _) => ExitSuccess,
            (true, false, _, _) => ExitBrokenWithoutViolation,
            (false, true, true, _) => ExitViolation,
            (false, true, false, _) => ExitPolicyViolation,
            (false, false, _, true) => ExitSuccess,
            (false, false, _, false) => ExitBudgetExhausted,
        };
    }

    private static IReadOnlyList<string>? FindPolicyPath(List<(ExploreProfile Profile, ExploreResult Result)> results, PolicyViolation policy)
    {
        for (var i = 0; i < results.Count; i++)
        {
            var result = results[i].Result;
            if (result.PolicyViolation != null && string.Equals(result.PolicyViolation.Policy, policy.Policy, StringComparison.Ordinal) &&
                string.Equals(result.PolicyViolation.StateFingerprint, policy.StateFingerprint, StringComparison.Ordinal))
                return result.CounterexamplePaths;
        }

        return null;
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
            BrokenMode.RepairCeiling => "RepairCeiling",
            BrokenMode.LogOrder => "LogOrder",
            BrokenMode.TransferHint => "TransferHint",
            BrokenMode.TransferHintStaleAccept => "TransferHintStaleAccept",
            _ => throw new ArgumentOutOfRangeException(nameof(broken), broken, "Unsupported enum value."),
        };
    }

    private static Task WriteSummaryAsync(string outputDir, in SummaryContent content, CancellationToken cancellationToken)
    {
        var sb = new StringBuilder(256);
        _ = sb.Append('{').Append("\"modelVersionHash\":");
        JsonText.AppendString(sb, ExploreRunner.ModelVersionHash);
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

        _ = sb.Append(",\"policyViolation\":");
        if (content.Policy == null)
        {
            _ = sb.Append("null");
        }
        else
        {
            _ = sb.Append('{').Append("\"policy\":");
            JsonText.AppendString(sb, content.Policy.Policy);
            _ = sb.Append(",\"detail\":");
            JsonText.AppendString(sb, content.Policy.Detail);
            _ = sb.Append(",\"event\":");
            JsonText.AppendString(sb, content.Policy.EventTrace);
            _ = sb.Append('}');
        }

        _ = sb.Append('}');
        return File.WriteAllTextAsync(Path.Join(outputDir, "summary.json"), sb.ToString(), Encoding.UTF8, cancellationToken);
    }

    [Immutable]
    private readonly record struct ResultAggregate(
        int TotalStates,
        int TotalTransitions,
        SafetyViolation? FirstViolation,
        ClusterState? FirstState,
        PolicyViolation? FirstPolicy,
        ClusterState? FirstPolicyState,
        bool AllFixedPoint);

    [Immutable]
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct SummaryContent(
        string ProfileName,
        BrokenMode Broken,
        int States,
        int Transitions,
        SafetyViolation? Violation,
        PolicyViolation? Policy,
        bool FixedPointReached);
}
