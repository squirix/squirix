using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Benchmarks;

/// <summary>Prints the stall measurement results as Markdown tables and timeline CSV files.</summary>
internal static class StallReport
{
    /// <summary>Gets a one-line summary of a cell.</summary>
    /// <param name="r">The result.</param>
    /// <returns>The line.</returns>
    internal static string Describe(StallCellResult r)
    {
        ArgumentNullException.ThrowIfNull(r);
        var errors = r.OtherErrors.Length > 0 ? ", errors: " + string.Join("; ", r.OtherErrors[..Math.Min(3, r.OtherErrors.Length)]) : string.Empty;
        return $"gc={r.Cell.GcLabel} stall={r.Cell.StallSeconds}s K={r.Cell.Writers} D={r.Cell.DeadlineSeconds}s: hold max {MeasurementOutput.Seconds(r.MaxHoldMs)} s, peak inflight {r.PeakInflight}, "
            + $"ok {r.Counts[StallOutcome.Ok]}/{r.WindowRequests}, recovery {MeasurementOutput.Seconds(r.RecoveryMs)} s, drain {MeasurementOutput.Seconds(r.DrainMs)} s{errors}";
    }

    /// <summary>Writes the sampled timeline of a cell as CSV.</summary>
    /// <param name="outDir">The output directory.</param>
    /// <param name="cell">The cell.</param>
    /// <param name="timeline">The samples.</param>
    /// <param name="stallStartMs">When the stall began.</param>
    /// <param name="stallEndMs">When the stall was released.</param>
    /// <returns>An asynchronous operation.</returns>
    internal static Task WriteTimelineAsync(string outDir, StallCellSpec cell, List<StallSample> timeline, double stallStartMs, double stallEndMs)
    {
        ArgumentNullException.ThrowIfNull(cell);
        _ = System.IO.Directory.CreateDirectory(outDir);
        var path = System.IO.Path.Join(outDir, $"timeline-{cell.Kind}-gc{cell.GcLabel}-k{cell.Writers}-d{cell.DeadlineSeconds}-s{cell.StallSeconds}.csv");
        var text = new StringBuilder();
        _ = text.AppendLine(CultureInfo.InvariantCulture, $"# stallStartMs={stallStartMs:F0} stallEndMs={stallEndMs:F0}");
        _ = text.AppendLine("timeMs,waitingAtGate,gateHeld,appendedAwaitingDurability,orphans,serverInflight,managedBytes,workingSetBytes,ioAgeMs");
        foreach (var s in CollectionsMarshal.AsSpan(timeline))
            _ = text.AppendLine(CultureInfo.InvariantCulture, $"{s.TimeMs:F0},{s.Waiting},{s.Held},{s.Appended},{s.Orphans},{s.ServerInflight},{s.ManagedBytes},{s.WorkingSetBytes},{s.IoAgeMs:F0}");

        return System.IO.File.WriteAllTextAsync(path, text.ToString(), CancellationToken.None);
    }

    /// <summary>Prints all tables.</summary>
    /// <param name="results">The results of every cell.</param>
    internal static void Print(List<StallCellResult> results)
    {
        ArgumentNullException.ThrowIfNull(results);
        var gate = new List<string>();
        var outcomes = new List<string>();
        var recovery = new List<string>();
        var failFast = new List<string>();
        foreach (var r in CollectionsMarshal.AsSpan(results))
        {
            gate.Add(GateRow(r));
            outcomes.Add(OutcomeRow(r));
            recovery.Add($"{r.Cell.Key()} {MeasurementOutput.Fixed0(r.BaselineOps)} | {MeasurementOutput.Seconds(r.RecoveryMs)} | {MeasurementOutput.Seconds(r.DrainMs)} |");
            foreach (var f in CollectionsMarshal.AsSpan(r.FailFast))
                failFast.Add(FailFastRow(r, f));
        }

        MeasurementOutput.Table(
            "Gate hold, queues and memory during the stall",
            "| group commit | stall s | writers | deadline s | longest gate hold s | gate holds > 1 s | peak waiting at gate | peak appended awaiting durability | peak server in-flight | peak orphans | probe max I/O age s | peak managed MB | peak working set MB |",
            gate);
        MeasurementOutput.Table("What writers saw (requests in flight at some point during the stall)", OutcomeHeader(), outcomes);
        MeasurementOutput.Table(
            "Recovery after the stall ends",
            "| group commit | stall s | writers | deadline s | baseline ops/s | throughput back to 80% after s | orphan backlog drained after s |",
            recovery);
        MeasurementOutput.Table(
            "Ideal fail-fast at threshold N on the same timeline (arrivals after N refused immediately)",
            "| group commit | stall s | writers | deadline s | N s | refused | refused but observed ok (collateral) | writer wait saved (client-s) | unknown outcomes avoided | failure wait p99 s observed to fail-fast | peak server in-flight observed to fail-fast |",
            failFast);
    }

    private static string GateRow(StallCellResult r) =>
        $"{r.Cell.Key()} {MeasurementOutput.Seconds(r.MaxHoldMs)} | {r.HoldsOverSecond} | {r.PeakWaiting} | {r.PeakAppended} | {r.PeakInflight} | {r.PeakOrphans} | {MeasurementOutput.Seconds(r.PeakIoAgeMs)} | +{MeasurementOutput.Fixed2(r.ManagedGrowthMb)} | +{MeasurementOutput.Fixed2(r.WorkingSetGrowthMb)} |";

    private static string OutcomeHeader()
    {
        var header = new StringBuilder("| group commit | stall s | writers | deadline s | requests |");
        for (var outcome = 0; outcome < StallOutcome.Count; outcome++)
            _ = header.Append(' ').Append(StallOutcome.Title(outcome)).Append(" |");

        return header.Append(" unknown that later committed | failure wait p50 s | p99 s | max s |").ToString();
    }

    private static string OutcomeRow(StallCellResult r)
    {
        var row = new StringBuilder(r.Cell.Key()).Append(' ').Append(r.WindowRequests).Append(" |");
        foreach (var count in r.Counts)
            _ = row.Append(' ').Append(count).Append(" |");

        return row.Append(CultureInfo.InvariantCulture, $" {r.LaterCommitted} | {MeasurementOutput.Seconds(r.FailureWaitP50Ms)} | {MeasurementOutput.Seconds(r.FailureWaitP99Ms)} | {MeasurementOutput.Seconds(r.FailureWaitMaxMs)} |").ToString();
    }

    private static string FailFastRow(StallCellResult r, StallFailFast f) =>
        $"{r.Cell.Key()} {MeasurementOutput.Fixed0(f.ThresholdSeconds)} | {f.Refused} | {f.Collateral} | {MeasurementOutput.Fixed0(f.SavedClientSeconds)} | {f.UnknownAvoided} | {MeasurementOutput.Seconds(f.ObservedFailureP99Ms)} to {MeasurementOutput.Seconds(f.FailFastFailureP99Ms)} | {f.ObservedPeakInflight} to {f.PeakInflight} |";
}
