using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Simulates a journal disk stall of S seconds in-process on the production journal path while K writers keep issuing durable mutations with a
/// client deadline D, and measures how long the mutation gate is held, how deep the queues get, how writers fail, how much memory grows, how long
/// recovery takes, and what an ideal fail-fast refusal after a threshold N would have given on the same timeline.
/// </summary>
internal static class JournalStallMeasurement
{
    private const int SamplePeriodMs = 100;

    /// <summary>Runs the stall matrix and prints Markdown tables.</summary>
    /// <param name="args">
    /// Options: --dir directory on the disk under test, --stall, --writers and --deadline comma lists in seconds or counts, --gc off, on or both,
    /// --kind flush or write, --pre and --post seconds around the stall, --payload bytes, --out directory for per-cell timeline CSV files.
    /// </param>
    /// <returns>An asynchronous operation.</returns>
    internal static async Task RunAsync(string[] args)
    {
        var options = MeasurementArgs.Parse(args);
        var root = options.GetString("--dir", Path.GetTempPath());
        var kind = string.Equals(options.GetString("--kind", "flush"), "write", StringComparison.Ordinal) ? JournalStallKind.Write : JournalStallKind.Flush;
        var outDir = options.GetString("--out", string.Empty);
        var cells = BuildCells(options, kind);
        MeasurementOutput.Line($"Journal stall measurement: dir={root}, kind={kind}, cells={cells.Count}");
        var results = new List<StallCellResult>();
        foreach (var cell in cells)
        {
            var result = await RunCellAsync(root, cell, outDir).ConfigureAwait(false);
            results.Add(result);
            MeasurementOutput.Line(StallReport.Describe(result));
        }

        StallReport.Print(results);
    }

    private static List<StallCellSpec> BuildCells(MeasurementArgs options, JournalStallKind kind)
    {
        bool[] gcModes = options.GetString("--gc", "both") switch
        {
            "off" => [false],
            "on" => [true],
            _ => [false, true],
        };
        var pre = options.GetInt("--pre", 3);
        var post = options.GetInt("--post", 8);
        var payload = options.GetInt("--payload", 1024);
        var cells = new List<StallCellSpec>();
        foreach (var groupCommit in gcModes)
            foreach (var stall in options.GetInts("--stall", [2, 5, 15]))
                foreach (var writers in options.GetInts("--writers", [8, 64, 256]))
                    foreach (var deadline in options.GetInts("--deadline", [2, 5]))
                        cells.Add(new StallCellSpec(groupCommit, writers, deadline, stall, kind, pre, post, payload));

        return cells;
    }

    private static async Task<StallCellResult> RunCellAsync(string root, StallCellSpec cell, string outDir)
    {
        var host = await JournalMeasurementHost.CreateAsync(root, cell.GroupCommit).ConfigureAwait(false);
        try
        {
            return await MeasureAsync(host, cell, outDir).ConfigureAwait(false);
        }
        finally
        {
            await host.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task<StallCellResult> MeasureAsync(JournalMeasurementHost host, StallCellSpec cell, string outDir)
    {
        var run = new StallRun(host, cell);
        var timeline = new List<StallSample>();
        using var stopSampling = new CancellationTokenSource();
        var sampler = SampleLoopAsync(run, host, timeline, stopSampling.Token);
        var writers = new Task[cell.Writers];
        for (var i = 0; i < cell.Writers; i++)
            writers[i] = run.WriterLoopAsync(i);

        await Task.Delay(TimeSpan.FromSeconds(cell.PreSeconds), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
        host.Writer.Arm(cell.Kind);
        await host.Writer.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
        var stallStartMs = run.NowMs();
        await Task.Delay(TimeSpan.FromSeconds(cell.StallSeconds), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
        host.Writer.Release();
        var stallEndMs = run.NowMs();
        await Task.Delay(TimeSpan.FromSeconds(cell.PostSeconds), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
        run.RequestStop();
        await Task.WhenAll(writers).ConfigureAwait(false);
        await run.WaitForServerIdleAsync(TimeSpan.FromSeconds(30)).ConfigureAwait(false);
        await stopSampling.CancelAsync().ConfigureAwait(false);
        await sampler.ConfigureAwait(false);

        var result = StallAnalysis.Analyze(cell, run, timeline, stallStartMs, stallEndMs);
        if (outDir.Length > 0)
            await StallReport.WriteTimelineAsync(outDir, cell, timeline, stallStartMs, stallEndMs).ConfigureAwait(false);

        return result;
    }

    private static async Task SampleLoopAsync(StallRun run, JournalMeasurementHost host, List<StallSample> timeline, CancellationToken cancellationToken)
    {
        await Task.Yield();
        using var process = Process.GetCurrentProcess();
        while (!cancellationToken.IsCancellationRequested)
        {
            process.Refresh();
            var ioAge = host.Journal.StallProbe.TryReadIo(out _, out var started) ? TimeProvider.System.GetElapsedTime(started).TotalMilliseconds : 0;
            timeline.Add(new StallSample(run.NowMs(), run.Waiting, run.Held, run.Appended, run.Orphans, run.ServerInflight, GC.GetTotalMemory(false), process.WorkingSet64, ioAge));
            try
            {
                await Task.Delay(TimeSpan.FromMilliseconds(SamplePeriodMs), TimeProvider.System, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }
}
