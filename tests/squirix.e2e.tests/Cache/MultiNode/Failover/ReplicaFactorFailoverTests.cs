using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// Groups of fewer than three replicas have no election path, and validation refuses automatic failover for them: losing a member never
/// raises a term, the calls the loss blocks are refused within the operation deadline, and the rest of the cluster keeps serving.
/// </summary>
public sealed class ReplicaFactorFailoverTests : EndToEndTestBase
{
    private const string CacheName = "rf-failover";

    /// <summary>The longest a refused call may take: the 15 s operation deadline of the SDK plus 5 s.</summary>
    private static readonly TimeSpan RefusalBound = TimeSpan.FromSeconds(20);

    /// <summary>Cancels a call that neither succeeds nor fails, well past <see cref="RefusalBound" />, so a hang fails the bound instead of the test timeout.</summary>
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(45);

    /// <summary>
    /// After the mirror of an RF=2 pair stops, no term rises over three election rounds: the survivor keeps leading its own group and never
    /// takes over the other. It still serves its committed value locally, and refuses writes to both groups within the deadline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task RfTwoDoesNotFailOverAfterOneMemberStops(CancellationToken cancellationToken)
    {
        var timing = FailoverTiming.For(nameof(RfTwoDoesNotFailOverAfterOneMemberStops));
        await using var cluster = await HostedCluster.StartTwoNodeAsync(Options(2, timing), nameof(RfTwoDoesNotFailOverAfterOneMemberStops), true, cancellationToken);
        var client = await cluster.ConnectClientAsync("nodeA", cancellationToken);
        var cache = await client.GetCacheAsync<long>(CacheName, cancellationToken);
        var kept = KeyOwnerHelper.TwoNode.FindKeyOwnedBy(CacheName, "nodeA", "rf2-kept");
        var lost = KeyOwnerHelper.TwoNode.FindKeyOwnedBy(CacheName, "nodeB", "rf2-lost");
        await cache.SetAsync(kept, 1L, cancellationToken: cancellationToken);
        await cache.SetAsync(lost, 1L, cancellationToken: cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);

        await cluster.StopNodeAsync("nodeB");
        var watch = Task.WhenAll(
            probe.AssertNoElectionAsync("nodeA", timing.Round * 3, cancellationToken),
            probe.AssertNoElectionAsync("nodeB", timing.Round * 3, false, cancellationToken));
        var local = await cache.GetValueAsync(kept, cancellationToken);
        var (keptFailure, keptElapsed) = await RefuseAsync((Cache: cache, Key: kept), static (s, token) => s.Cache.SetAsync(s.Key, 2L, cancellationToken: token), cancellationToken);
        var (lostFailure, lostElapsed) = await RefuseAsync((Cache: cache, Key: lost), static (s, token) => s.Cache.SetAsync(s.Key, 2L, cancellationToken: token), cancellationToken);
        await watch;

        // The refusals may outlast the watch: observe once more, so authority gained during them is counted and checked as well.
        _ = probe.Ledger("nodeA").Observe();
        _ = probe.Ledger("nodeB").Observe();
        _ = await Assert.That(local).IsEqualTo(new CacheValueResult<long>(true, 1L));
        _ = await Assert.That(keptElapsed).IsLessThanOrEqualTo(RefusalBound).Because(keptFailure.ToString());
        _ = await Assert.That(IsRefusal(keptFailure)).IsTrue().Because(keptFailure.ToString());
        _ = await Assert.That(lostElapsed).IsLessThanOrEqualTo(RefusalBound).Because(lostFailure.ToString());
        _ = await Assert.That(IsRefusal(lostFailure)).IsTrue().Because(lostFailure.ToString());
        _ = await Assert.That(probe.Ledger("nodeA").Terms).IsEqualTo(1);
        _ = await Assert.That(probe.Ledger("nodeB").Terms).IsEqualTo(0);
    }

    /// <summary>
    /// After the owner of an RF=1 group stops, no node takes its group over: its keys are refused within the deadline, while the keys of the
    /// running owners keep a clean register history with no failed call while the refusals run.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task RfOneHasNoElectionPath(CancellationToken cancellationToken)
    {
        var timing = FailoverTiming.For(nameof(RfOneHasNoElectionPath));
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(RfOneHasNoElectionPath), Options(1, timing), true, cancellationToken);
        var writer = await cluster.ConnectClientAsync("nodeB", cancellationToken);
        var reader = await cluster.ConnectClientAsync("nodeC", cancellationToken);
        var cache = await writer.GetCacheAsync<long>(CacheName, cancellationToken);
        var lost = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, "nodeA", "rf1-lost");
        string[] kept = [KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, "nodeB", "rf1-kept"), KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, "nodeC", "rf1-kept")];
        await cache.SetAsync(lost, 1L, cancellationToken: cancellationToken);
        var workload = new RegisterWorkload(cache, await reader.GetCacheAsync<long>(CacheName, cancellationToken), kept);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);

        await cluster.StopNodeAsync("nodeA");
        var watch = probe.AssertNoElectionAsync("nodeA", timing.Round * 3, false, cancellationToken);
        var unaffected = workload.RunAsync(100, 100, cancellationToken);
        var (writeFailure, writeElapsed) = await RefuseAsync((Cache: cache, Key: lost), static (s, token) => s.Cache.SetAsync(s.Key, 2L, cancellationToken: token), cancellationToken);
        var (readFailure, readElapsed) = await RefuseAsync(
            (Cache: cache, Key: lost),
            static async (s, token) => _ = await s.Cache.GetValueAsync(s.Key, token),
            cancellationToken);
        await Task.WhenAll(unaffected, watch);

        _ = probe.Ledger("nodeA").Observe();
        var history = workload.History;
        _ = await Assert.That(writeElapsed).IsLessThanOrEqualTo(RefusalBound).Because(writeFailure.ToString());
        _ = await Assert.That(IsRefusal(writeFailure)).IsTrue().Because(writeFailure.ToString());
        _ = await Assert.That(readElapsed).IsLessThanOrEqualTo(RefusalBound).Because(readFailure.ToString());
        _ = await Assert.That(IsRefusal(readFailure)).IsTrue().Because(readFailure.ToString());
        _ = await Assert.That(history.Check()).IsEmpty().Because(history.Summary());
        _ = await Assert.That((history.AmbiguousWrites, history.FailedReads)).IsEqualTo((0, 0)).Because(history.Summary());
        _ = await Assert.That(probe.Ledger("nodeA").Terms).IsEqualTo(0);
    }

    /// <summary>Tells whether a failure is a product refusal: an RPC status or an unknown commit outcome, not a client-side cancellation.</summary>
    /// <param name="failure">The failure of the call.</param>
    /// <returns><see langword="true" /> when the product refused the call.</returns>
    private static bool IsRefusal(Exception failure) => failure is RpcException { StatusCode: not StatusCode.Cancelled } or CommitOutcomeUnknownException;

    private static MultiNodeStartOptions Options(int replicaCount, TestElectionTiming timing) => new()
    {
        ReplicaCount = replicaCount,
        ElectionTiming = timing,
    };

    /// <summary>Runs a call that must fail, and measures how long it took to fail.</summary>
    /// <typeparam name="TState">The type of the state the call reads.</typeparam>
    /// <param name="state">The state the call reads.</param>
    /// <param name="call">The call.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The failure and the time from the start of the call to its failure.</returns>
    private static async Task<(Exception Failure, TimeSpan Elapsed)> RefuseAsync<TState>(TState state, Func<TState, CancellationToken, Task> call, CancellationToken cancellationToken)
    {
        using var guard = new CancellationTokenSource(HangGuard);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, guard.Token);
        var started = Stopwatch.GetTimestamp();
        var failure = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(call(state, linked.Token));
        return (failure, Stopwatch.GetElapsedTime(started));
    }
}
