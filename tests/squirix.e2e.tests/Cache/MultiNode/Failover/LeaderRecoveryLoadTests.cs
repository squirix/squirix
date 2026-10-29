using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>Manual mixed-load evidence for SDK requests across a leader loss, without application retries.</summary>
[Explicit]
public sealed class LeaderRecoveryLoadTests : EndToEndTestBase
{
    private const int BatchSize = 30;
    private const int Batches = 200;
    private const string CacheName = "leader-recovery-load";
    private const string Group = "nodeA";

    /// <summary>Offers 300 requests per second and stops a leader while the workload continues.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task MixedLoadSurvivesLeaderLoss(CancellationToken cancellationToken)
    {
        const string testName = nameof(MixedLoadSurvivesLeaderLoss);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName, timing: FailoverTiming.ProductDefaults(testName)), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var cache = await cluster.GetCacheAsync<long>(CacheName, survivors[0], cancellationToken);
        var (keys, reads) = await SeedAsync(cache, cancellationToken);

        var durations = new double[Batches * BatchSize];
        var failures = new ConcurrentQueue<string>();
        var calls = new List<Task>(durations.Length);
        Task stopping = Task.CompletedTask;
        var started = Stopwatch.GetTimestamp();
        TimeSpan generated;
        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
        try
        {
            for (var batch = 0; batch < Batches; batch++)
            {
                _ = await timer.WaitForNextTickAsync(cancellationToken);
                if (batch == 50)
                    stopping = cluster.AbruptShutdownNodeAsync(former).AsTask();

                for (var offset = 0; offset < BatchSize; offset++)
                {
                    var index = (batch * BatchSize) + offset;
                    var group = index % 3;
                    var write = index % 2 == 0;
                    var key = write ? keys[group][index / 6] : reads[group];
                    calls.Add(InvokeAsync(cache, (key, index, write), durations, failures, cancellationToken));
                }
            }

            generated = Stopwatch.GetElapsedTime(started);
        }
        finally
        {
            // Every started request is joined before the cluster can be disposed, including when scheduling is canceled.
            await Task.WhenAll(Task.WhenAll(calls), stopping);
        }

        var elapsed = Stopwatch.GetElapsedTime(started);
        await cluster.StopNodeAsync(former);
        var (leader, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
        var audit = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);
        Array.Sort(durations);
        Console.WriteLine($"requests={calls.Count}, failures={failures.Count}, offered_rps={calls.Count / generated.TotalSeconds:F1}, elapsed_s={elapsed.TotalSeconds:F2}, p99_ms={durations[(durations.Length * 99 / 100) - 1]:F2}, max_ms={durations[^1]:F2}");

        _ = await Assert.That(failures.Count).IsEqualTo(0).Because(string.Join(Environment.NewLine, failures));
        _ = await Assert.That(audit.ClientEntries).IsEqualTo(keys[0].Length + 1);
        _ = await Assert.That(term).IsGreaterThan(formerTerm);
        _ = await Assert.That(leader).IsNotEqualTo(former);
    }

    private static async Task<(string[][] Writes, string[] Reads)> SeedAsync(ICache<long> cache, CancellationToken cancellationToken)
    {
        var keys = new string[3][];
        var reads = new string[3];
        for (var i = 0; i < keys.Length; i++)
        {
            var group = FailoverSteps.ThreeNodes[i];
            keys[i] = FailoverSteps.KeysOf(CacheName, group, "write", Batches * BatchSize / 6);
            reads[i] = FailoverSteps.KeysOf(CacheName, group, "read", 1)[0];
            await cache.SetAsync(reads[i], 1L, cancellationToken: cancellationToken);
        }

        return (keys, reads);
    }

    private static async Task InvokeAsync(ICache<long> cache, (string Key, int Index, bool Write) operation, double[] durations, ConcurrentQueue<string> failures, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        try
        {
            if (operation.Write)
            {
                await cache.SetAsync(operation.Key, operation.Index, cancellationToken: cancellationToken);
            }
            else
            {
                var read = await cache.GetValueAsync(operation.Key, cancellationToken);
                _ = await Assert.That(read).IsEqualTo(new CacheValueResult<long>(true, 1L));
            }
        }
        catch (Exception exception) when (exception is RpcException or CommitOutcomeUnknownException)
        {
            failures.Enqueue($"{operation.Index}: {exception.Message}");
        }
        finally
        {
            durations[operation.Index] = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        }
    }
}
