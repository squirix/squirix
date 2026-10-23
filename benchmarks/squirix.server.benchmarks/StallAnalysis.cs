using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Squirix.Server.Benchmarks;

/// <summary>Derives the stall measurement results from the recorded requests and the sampled timeline.</summary>
internal static class StallAnalysis
{
    private const double BaselineFromMs = 1000;
    private const double BytesPerMegabyte = 1024.0 * 1024.0;

    private static ReadOnlySpan<double> FailFastThresholdsSeconds => [1, 2, 5];

    /// <summary>Analyzes one finished cell.</summary>
    /// <param name="cell">The measured cell.</param>
    /// <param name="run">The finished run.</param>
    /// <param name="timeline">The sampled timeline.</param>
    /// <param name="stallStartMs">When the stall began, in milliseconds since the run started.</param>
    /// <param name="stallEndMs">When the stall was released.</param>
    /// <returns>The cell result.</returns>
    internal static StallCellResult Analyze(StallCellSpec cell, StallRun run, List<StallSample> timeline, double stallStartMs, double stallEndMs)
    {
        var result = new StallCellResult(cell) { StallMs = stallEndMs - stallStartMs };
        StallRequest[] requests = [.. run.Requests];
        double[] okTimes = [.. run.OkCompletionMs];
        Array.Sort(okTimes);
        MeasureThroughput(result, okTimes, stallStartMs, stallEndMs);
        MeasureDrain(result, timeline, stallEndMs);
        MeasureQueuesAndMemory(result, timeline, stallStartMs);
        MeasureGateHolds(result, [.. run.Holds]);
        var window = SelectWindow(requests, stallStartMs, stallEndMs);
        MeasureWindow(result, window);
        foreach (var threshold in FailFastThresholdsSeconds)
        {
            if (threshold * 1000 < result.StallMs)
                result.FailFast.Add(SimulateFailFast(requests, window, stallStartMs, threshold));
        }

        result.OtherErrors = [.. run.OtherErrors];
        return result;
    }

    private static void MeasureThroughput(StallCellResult result, double[] okTimes, double stallStartMs, double stallEndMs)
    {
        var baselineSeconds = Math.Max((stallStartMs - BaselineFromMs) / 1000.0, 0.001);
        result.BaselineOps = CountBetween(okTimes, BaselineFromMs, stallStartMs) / baselineSeconds;
        var lastStart = stallEndMs + (result.Cell.PostSeconds * 1000.0) - 1000;
        for (var t = stallEndMs; t < lastStart; t += 50)
        {
            var first = CountBetween(okTimes, t, t + 500) / 0.5;
            var second = CountBetween(okTimes, t + 500, t + 1000) / 0.5;
            if (first < 0.8 * result.BaselineOps || second < 0.8 * result.BaselineOps)
                continue;

            result.RecoveryMs = t - stallEndMs;
            return;
        }
    }

    private static void MeasureDrain(StallCellResult result, List<StallSample> timeline, double stallEndMs)
    {
        foreach (var sample in CollectionsMarshal.AsSpan(timeline))
        {
            if (sample.TimeMs < stallEndMs || sample.Orphans != 0)
                continue;

            result.DrainMs = sample.TimeMs - stallEndMs;
            return;
        }
    }

    private static void MeasureQueuesAndMemory(StallCellResult result, List<StallSample> timeline, double stallStartMs)
    {
        double baselineManaged = 0;
        double baselineWorkingSet = 0;
        var baselineCount = 0;
        foreach (var s in CollectionsMarshal.AsSpan(timeline))
        {
            if (s.TimeMs < BaselineFromMs || s.TimeMs >= stallStartMs)
                continue;

            baselineManaged += s.ManagedBytes;
            baselineWorkingSet += s.WorkingSetBytes;
            baselineCount++;
        }

        if (baselineCount > 0)
        {
            baselineManaged /= baselineCount;
            baselineWorkingSet /= baselineCount;
        }

        double peakManaged = 0;
        double peakWorkingSet = 0;
        foreach (var s in CollectionsMarshal.AsSpan(timeline))
        {
            result.PeakWaiting = Math.Max(result.PeakWaiting, s.Waiting);
            result.PeakAppended = Math.Max(result.PeakAppended, s.Appended);
            result.PeakInflight = Math.Max(result.PeakInflight, s.ServerInflight);
            result.PeakOrphans = Math.Max(result.PeakOrphans, s.Orphans);
            result.PeakIoAgeMs = Math.Max(result.PeakIoAgeMs, s.IoAgeMs);
            peakManaged = Math.Max(peakManaged, s.ManagedBytes);
            peakWorkingSet = Math.Max(peakWorkingSet, s.WorkingSetBytes);
        }

        result.ManagedGrowthMb = Math.Max(0, peakManaged - baselineManaged) / BytesPerMegabyte;
        result.WorkingSetGrowthMb = Math.Max(0, peakWorkingSet - baselineWorkingSet) / BytesPerMegabyte;
    }

    private static void MeasureGateHolds(StallCellResult result, double[] holds)
    {
        foreach (var hold in holds)
        {
            result.MaxHoldMs = Math.Max(result.MaxHoldMs, hold);
            if (hold > 1000)
                result.HoldsOverSecond++;
        }
    }

    private static List<StallRequest> SelectWindow(StallRequest[] requests, double stallStartMs, double stallEndMs)
    {
        var window = new List<StallRequest>();
        foreach (var request in requests)
        {
            if (request.IssueMs <= stallEndMs && request.ClientEndMs >= stallStartMs)
                window.Add(request);
        }

        return window;
    }

    private static void MeasureWindow(StallCellResult result, List<StallRequest> window)
    {
        var failureWaits = new List<double>();
        foreach (var request in CollectionsMarshal.AsSpan(window))
        {
            result.Counts[request.Outcome]++;
            if (request.Outcome != StallOutcome.Ok)
                failureWaits.Add(request.ClientEndMs - request.IssueMs);

            var timedOut = request.Outcome is StallOutcome.TimedOutFrameQueued or StallOutcome.TimedOutNotAppended;
            if (timedOut && request.CommittedLater)
                result.LaterCommitted++;
        }

        result.WindowRequests = window.Count;
        result.FailureWaitP50Ms = Percentile(failureWaits, 0.5);
        result.FailureWaitP99Ms = Percentile(failureWaits, 0.99);
        result.FailureWaitMaxMs = Percentile(failureWaits, 1.0);
    }

    private static StallFailFast SimulateFailFast(StallRequest[] requests, List<StallRequest> window, double stallStartMs, double thresholdSeconds)
    {
        var cutoff = stallStartMs + (thresholdSeconds * 1000);
        var refusedRequests = new HashSet<StallRequest>();
        var collateral = 0;
        var unknownAvoided = 0;
        var savedSeconds = 0.0;
        var failFastWaits = new List<double>();
        var observedWaits = new List<double>();
        foreach (var request in CollectionsMarshal.AsSpan(window))
        {
            var observedWait = request.ClientEndMs - request.IssueMs;
            if (request.Outcome != StallOutcome.Ok)
                observedWaits.Add(observedWait);

            if (request.IssueMs < cutoff)
            {
                if (request.Outcome != StallOutcome.Ok)
                    failFastWaits.Add(observedWait);

                continue;
            }

            _ = refusedRequests.Add(request);
            failFastWaits.Add(0);
            savedSeconds += observedWait / 1000.0;
            if (request.Outcome == StallOutcome.Ok)
                collateral++;

            if (request.Outcome == StallOutcome.TimedOutFrameQueued)
                unknownAvoided++;
        }

        var peak = PeakInflightWithout(requests, refusedRequests);
        var observedPeak = PeakInflightWithout(requests, []);
        return new StallFailFast(thresholdSeconds, refusedRequests.Count, collateral, savedSeconds, Percentile(failFastWaits, 0.99), Percentile(observedWaits, 0.99), unknownAvoided, peak, observedPeak);
    }

    private static int PeakInflightWithout(StallRequest[] requests, HashSet<StallRequest> refused)
    {
        var events = new List<(double Time, int Delta)>();
        foreach (var request in requests)
        {
            if (refused.Contains(request))
                continue;

            events.Add((request.IssueMs, 1));
            events.Add((request.ServerEndMs, -1));
        }

        events.Sort(static (a, b) => a.Time.CompareTo(b.Time) is var byTime and not 0 ? byTime : a.Delta.CompareTo(b.Delta));
        var current = 0;
        var peak = 0;
        foreach (var (_, delta) in CollectionsMarshal.AsSpan(events))
        {
            current += delta;
            peak = Math.Max(peak, current);
        }

        return peak;
    }

    private static int CountBetween(double[] sorted, double fromMs, double toMs)
    {
        var count = 0;
        foreach (var value in sorted)
        {
            if (value >= fromMs && value < toMs)
                count++;
        }

        return count;
    }

    private static double Percentile(List<double> values, double fraction)
    {
        if (values.Count == 0)
            return 0;

        double[] sorted = [.. values];
        Array.Sort(sorted);
        var rank = Convert.ToInt32(Math.Ceiling(fraction * sorted.Length));
        return sorted[Math.Clamp(rank - 1, 0, sorted.Length - 1)];
    }
}
