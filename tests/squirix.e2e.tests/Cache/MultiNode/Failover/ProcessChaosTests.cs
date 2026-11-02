using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Client;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>Public-SDK workloads for the isolated Linux process cluster managed by tools/chaos/run.py.</summary>
[Property("Suite", "ProcessChaos")]
public sealed class ProcessChaosTests
{
    /// <summary>Checks histories across every externally controlled fault and again after all nodes recover.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    [Explicit]
    [Timeout(900_000)]
    public async Task ProcessFaultsKeepRegistersLinearizable(CancellationToken cancellationToken)
    {
        var control = Required("SQUIRIX_CHAOS_CONTROL");
        var endpoints = Required("SQUIRIX_CHAOS_ENDPOINTS").Split(';');
        var steps = int.Parse(Required("SQUIRIX_CHAOS_STEPS"), CultureInfo.InvariantCulture);
        await using var writer = await ConnectAsync(endpoints, cancellationToken);
        await using var reader = await ConnectAsync(endpoints, cancellationToken);
        var writeCache = await writer.GetCacheAsync<long>("process-chaos", cancellationToken);
        var readCache = await reader.GetCacheAsync<long>("process-chaos", cancellationToken);
        var histories = new List<(RegisterHistory History, string[] Keys, string Path)>();
        for (var step = 0; step < steps; step++)
        {
            var prefix = Path.Join(control, step.ToString(CultureInfo.InvariantCulture));
            var keys = KeysForStep(endpoints.Length, step);
            var workload = new RegisterWorkload(writeCache, readCache, keys) { Pace = TimeSpan.FromMilliseconds(100) };
            var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var running = workload.RunUntilAsync(done.Task, 10, stop.Token);
            histories.Add((workload.History, keys, prefix + ".history.json"));
            var recoveredAt = 0L;
            try
            {
                await WaitForProgressAsync(workload.History, running, control, cancellationToken);
                await File.WriteAllTextAsync(prefix + ".ready", "ready", cancellationToken);
                await WaitForFileAsync(prefix + ".done", cancellationToken);
                recoveredAt = Stopwatch.GetTimestamp();
            }
            catch
            {
                await stop.CancelAsync();
                throw;
            }
            finally
            {
                _ = done.TrySetResult();
                try
                {
                    await running;
                }
                finally
                {
                    await RegisterHistoryFile.SaveAsync(prefix + ".history.json", workload.History, CancellationToken.None);
                }
            }

            foreach (var key in keys)
                _ = await Assert.That(workload.History.HasAcknowledgedWriteSince(key, recoveredAt)).IsTrue().Because($"{prefix}: key {key} must accept a write after recovery");
            var (_, reads) = workload.History.StartedAfter(0);
            _ = await Assert.That(reads).IsGreaterThan(0);
            _ = await Assert.That(workload.History.Check(cancellationToken)).IsEmpty().Because(prefix);
        }

        // Every endpoint must recover, and old acknowledgements must survive the final whole-cluster restart.
        foreach (var endpoint in endpoints)
        {
            await using var recovered = await ConnectAsync([endpoint], cancellationToken);
            var cache = await recovered.GetCacheAsync<long>("process-chaos", cancellationToken);
            foreach (var history in histories)
                await VerifyHistoryAsync(cache, history, cancellationToken);
        }
    }

    /// <summary>Replays a saved artifact without starting any server.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    [Explicit]
    public async Task SavedHistoryHasLegalOrder(CancellationToken cancellationToken) =>
        _ = await Assert.That(await RegisterHistoryFile.CheckAsync(Required("SQUIRIX_CHAOS_HISTORY"), cancellationToken)).IsEmpty();

    private static string Required(string name) => Environment.GetEnvironmentVariable(name) ?? Missing(name);

    private static string Missing(string name) => throw new InvalidOperationException($"{name} must be set by the process chaos runner.");

    private static string[] KeysForStep(int replicas, int step)
    {
        var nodes = new string[replicas];
        for (var i = 0; i < nodes.Length; i++)
            nodes[i] = "n" + i.ToString(CultureInfo.InvariantCulture);
        var ring = new TestKeyOwnerHelper(nodes);
        var keys = new string[replicas];
        for (var i = 0; i < keys.Length; i++)
            keys[i] = ring.FindKeyOwnedBy("process-chaos", nodes[i], $"step-{step}-{i}-");
        return keys;
    }

    private static async Task VerifyHistoryAsync(ICache<long> cache, (RegisterHistory History, string[] Keys, string Path) entry, CancellationToken cancellationToken)
    {
        var (history, keys, path) = entry;
        try
        {
            foreach (var key in keys)
                await ReadRecoveredAsync(cache, key, history, cancellationToken);
        }
        finally
        {
            await RegisterHistoryFile.SaveAsync(path, history, CancellationToken.None);
        }

        _ = await Assert.That(history.Check(cancellationToken)).IsEmpty().Because(path);
    }

    private static async Task ReadRecoveredAsync(ICache<long> cache, string key, RegisterHistory history, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(1));
        while (true)
        {
            var start = Stopwatch.GetTimestamp();
            try
            {
                var value = await cache.GetValueAsync(key, timeout.Token);
                history.RecordRead(new RegisterRead(key, start, Stopwatch.GetTimestamp(), value.Found ? value.Value : 0));
                return;
            }
            catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded)
            {
                history.RecordReadFailure(key, start, Stopwatch.GetTimestamp());
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, timeout.Token);
        }
    }

    private static ValueTask<ISquirixClient> ConnectAsync(string[] endpoints, CancellationToken cancellationToken) =>
        SquirixClient.ConnectAsync(
            options =>
            {
                foreach (var endpoint in endpoints)
                    options.Endpoints.Add(new Uri(endpoint));
                var token = Required("SQUIRIX_CHAOS_TOKEN");
                options.BearerTokenProvider = _ => ValueTask.FromResult(token);
            },
            cancellationToken);

    private static async Task WaitForFileAsync(string path, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(3));
        while (!File.Exists(path))
        {
            CheckAbort(Path.GetDirectoryName(path)!);
            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, timeout.Token);
        }
    }

    private static void CheckAbort(string control)
    {
        if (File.Exists(Path.Join(control, "abort")))
            throw new InvalidOperationException("The process fault controller aborted the run.");
    }

    private static async Task WaitForProgressAsync(RegisterHistory history, Task running, string control, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromMinutes(1));
        while (history.StartedAfter(0) is var progress && (progress.AckedWrites < 3 || progress.Reads < 3))
        {
            CheckAbort(control);
            if (running.IsCompleted)
                await running;
            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, timeout.Token);
        }
    }
}
