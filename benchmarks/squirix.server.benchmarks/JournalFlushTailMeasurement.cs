using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// Measures the distribution of journal segment write and flush durations and of the end-to-end durable append latency on a real disk through the
/// production journal path, and what the journal stall probe would have reported meanwhile. A console runner rather than BenchmarkDotNet because every
/// sample is kept, so tail percentiles are exact.
/// </summary>
internal static class JournalFlushTailMeasurement
{
    private static readonly TimeSpan[] Thresholds =
    [
        TimeSpan.FromMilliseconds(100),
        TimeSpan.FromMilliseconds(250),
        TimeSpan.FromMilliseconds(500),
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(2),
    ];

    /// <summary>Runs the measurement matrix and prints Markdown tables.</summary>
    /// <param name="args">
    /// Options: --dir directory on the disk under test, --seconds measured seconds per scenario, --warmup warmup seconds, --payloads and --writers
    /// comma lists, --noise noisy writer counts (0 is a quiet disk), --gc off, on or both.
    /// </param>
    /// <returns>An asynchronous operation.</returns>
    internal static async Task RunAsync(string[] args)
    {
        var options = MeasurementArgs.Parse(args);
        var root = options.GetString("--dir", Path.GetTempPath());
        var seconds = options.GetInt("--seconds", 15);
        var warmup = options.GetInt("--warmup", 2);
        var scenarios = BuildScenarios(
            options.GetInts("--noise", [0, 2]),
            options.GetString("--gc", "both"),
            options.GetInts("--payloads", [128, 1024, 65536]),
            options.GetInts("--writers", [1, 8, 64]));

        MeasurementOutput.Line($"Journal flush tail measurement: dir={root}, measured={seconds}s, warmup={warmup}s, scenarios={scenarios.Count}");
        var results = new List<TailResult>();
        foreach (var scenario in scenarios)
        {
            var result = await RunScenarioAsync(root, scenario, warmup, seconds).ConfigureAwait(false);
            results.Add(result);
            MeasurementOutput.Line(
                $"{result.Key()} {MeasurementOutput.Fixed0(result.OpsPerSecond)} ops/s, e2e p99 {MeasurementOutput.Fixed2(result.EndToEnd.Percentile(0.99))} ms, e2e max {MeasurementOutput.Fixed2(result.EndToEnd.MaxMs())} ms");
        }

        PrintTables(results);
    }

    private static List<TailScenario> BuildScenarios(int[] noises, string gcMode, int[] payloads, int[] writers)
    {
        bool[] gcModes = gcMode switch
        {
            "off" => [false],
            "on" => [true],
            _ => [false, true],
        };
        var scenarios = new List<TailScenario>();
        foreach (var noise in noises)
            foreach (var groupCommit in gcModes)
                foreach (var payload in payloads)
                    foreach (var writerCount in writers)
                        scenarios.Add(new TailScenario(groupCommit, writerCount, payload, noise));

        return scenarios;
    }

    private static async Task<TailResult> RunScenarioAsync(string root, TailScenario scenario, int warmupSeconds, int seconds)
    {
        var host = await JournalMeasurementHost.CreateAsync(root, scenario.GroupCommit).ConfigureAwait(false);
        var noise = DiskNoise.Start(root, scenario.NoiseWriters);
        try
        {
            return await MeasureAsync(host, scenario, warmupSeconds, seconds).ConfigureAwait(false);
        }
        finally
        {
            noise.Dispose();
            await host.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task<TailResult> MeasureAsync(JournalMeasurementHost host, TailScenario scenario, int warmupSeconds, int seconds)
    {
        using var stop = new CancellationTokenSource();
        return await MeasureCoreAsync(host, scenario, warmupSeconds, seconds, stop).ConfigureAwait(false);
    }

    private static async Task<TailResult> MeasureCoreAsync(JournalMeasurementHost host, TailScenario scenario, int warmupSeconds, int seconds, CancellationTokenSource stop)
    {
        var writers = new TailWriters(host, scenario.PayloadBytes);
        var perWriter = new LatencySamples[scenario.Writers];
        var tasks = new Task[scenario.Writers];
        for (var i = 0; i < scenario.Writers; i++)
        {
            perWriter[i] = new LatencySamples();
            tasks[i] = writers.RunAsync(i, perWriter[i], stop.Token);
        }

        var probe = new ProbeSampler(host.Journal.StallProbe);
        await Task.Delay(TimeSpan.FromSeconds(warmupSeconds), TimeProvider.System, stop.Token).ConfigureAwait(false);
        host.Writer.SetRecording(true);
        probe.Start();
        writers.SetRecording(true);
        var started = TimeProvider.System.GetTimestamp();
        await Task.Delay(TimeSpan.FromSeconds(seconds), TimeProvider.System, stop.Token).ConfigureAwait(false);
        writers.SetRecording(false);
        var elapsed = TimeProvider.System.GetElapsedTime(started);
        host.Writer.SetRecording(false);
        probe.Stop();
        await stop.CancelAsync().ConfigureAwait(false);
        await Task.WhenAll(tasks).ConfigureAwait(false);

        var endToEnd = new LatencySamples();
        foreach (var samples in perWriter)
            endToEnd.AddRange(samples);

        return new TailResult(scenario, elapsed, endToEnd, host.Writer.Flushes, host.Writer.Writes, probe.ReadMaxAge());
    }

    private static void PrintTables(List<TailResult> results)
    {
        var endToEnd = new List<string>();
        var flushes = new List<string>();
        var writes = new List<string>();
        var exceedance = new List<string>();
        foreach (var r in CollectionsMarshal.AsSpan(results))
        {
            endToEnd.Add($"{r.Key()} {MeasurementOutput.Fixed0(r.OpsPerSecond)} | {Percentiles(r.EndToEnd)} |");
            flushes.Add($"{r.Key()} {r.Flushes.Count} | {Percentiles(r.Flushes)} |");
            writes.Add($"{r.Key()} {r.Writes.Count} | {Percentiles(r.Writes)} |");
            exceedance.Add($"{r.Key()} {r.Flushes.Count + r.Writes.Count} | {IoExceedance(r)} | {Exceedance(r.EndToEnd)} | {MeasurementOutput.Fixed2(r.ProbeMaxAge.TotalMilliseconds)} |");
        }

        MeasurementOutput.Table("End-to-end durable append latency (ms)", "| noise | group commit | payload | writers | ops/s | p50 | p99 | p99.9 | max |", endToEnd);
        MeasurementOutput.Table("Journal flush (FlushToDisk) duration (ms)", "| noise | group commit | payload | writers | flushes | p50 | p99 | p99.9 | max |", flushes);
        MeasurementOutput.Table("Journal segment write (write-through) duration (ms)", "| noise | group commit | payload | writers | writes | p50 | p99 | p99.9 | max |", writes);
        MeasurementOutput.Table(
            "Single I/O calls (write or flush) above a threshold, end-to-end appends above a threshold, and the longest I/O call the stall probe saw in progress (ms)",
            "| noise | group commit | payload | writers | I/O calls | io > 100 ms | io > 250 ms | io > 500 ms | io > 1 s | io > 2 s | e2e > 100 ms | e2e > 250 ms | e2e > 500 ms | e2e > 1 s | e2e > 2 s | probe max |",
            exceedance);
    }

    private static string Percentiles(LatencySamples samples) =>
        $"{MeasurementOutput.Fixed2(samples.Percentile(0.5))} | {MeasurementOutput.Fixed2(samples.Percentile(0.99))} | {MeasurementOutput.Fixed2(samples.Percentile(0.999))} | {MeasurementOutput.Fixed2(samples.MaxMs())}";

    private static string Exceedance(LatencySamples samples)
    {
        var cells = new string[Thresholds.Length];
        for (var i = 0; i < Thresholds.Length; i++)
            cells[i] = samples.CountAbove(Thresholds[i]).ToString(System.Globalization.CultureInfo.InvariantCulture);

        return string.Join(" | ", cells);
    }

    private static string IoExceedance(TailResult result)
    {
        var cells = new string[Thresholds.Length];
        for (var i = 0; i < Thresholds.Length; i++)
            cells[i] = (result.Flushes.CountAbove(Thresholds[i]) + result.Writes.CountAbove(Thresholds[i])).ToString(System.Globalization.CultureInfo.InvariantCulture);

        return string.Join(" | ", cells);
    }
}
