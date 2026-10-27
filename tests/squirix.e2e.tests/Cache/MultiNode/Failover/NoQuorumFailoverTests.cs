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
/// With two of three replicas gone no majority exists: a write through the surviving node, whether it led the group or followed it, ends
/// with a failure the client can retry within the operation deadline of the SDK, and is never acknowledged.
/// </summary>
public sealed class NoQuorumFailoverTests : EndToEndTestBase
{
    private const string CacheName = "no-quorum";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    /// <summary>How far past the operation deadline a failure may arrive on a loaded agent.</summary>
    private static readonly TimeSpan Margin = TimeSpan.FromSeconds(10);

    /// <summary>The operation deadline of the SDK: five per-attempt timeouts of three seconds, shared by every attempt of one call.</summary>
    private static readonly TimeSpan OperationDeadline = TimeSpan.FromSeconds(15);

    /// <summary>The per-attempt timeout of the SDK, which the SDK applies as a cancellation token and the server never sees; mirrors the internal per-attempt timeout of the SDK.</summary>
    private static readonly TimeSpan AttemptTimeout = TimeSpan.FromSeconds(3);

    /// <summary>The surviving node led the group, so it alone appends the write and never gets a majority for it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public Task SurvivingLeaderFailsWrites(CancellationToken cancellationToken) => WriteFailsAsync(nameof(SurvivingLeaderFailsWrites), true, cancellationToken);

    /// <summary>
    /// The surviving node followed the group, so no leader exists to commit the write. Once the node knows no leader, a read through it is
    /// refused as unavailable by the node itself, within one per-attempt timeout of the SDK, so the SDK reports the refusal and not a timeout.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public Task SurvivingFollowerFailsWrites(CancellationToken cancellationToken) => WriteFailsAsync(nameof(SurvivingFollowerFailsWrites), false, cancellationToken);

    /// <summary>
    /// Waits until the surviving follower knows no leader, then reads once through the wire the way the SDK does: with the operation deadline,
    /// and a token that ends the attempt after one per-attempt timeout. The node must refuse as unavailable by itself before the token fires,
    /// which would end the call as cancelled.
    /// </summary>
    /// <param name="cluster">The cluster.</param>
    /// <param name="probe">The leader probe of the cluster.</param>
    /// <param name="survivor">The surviving follower.</param>
    /// <param name="key">A key of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous check.</returns>
    private static async Task AssertRefusesReadInOneAttemptAsync(HostedCluster cluster, ClusterLeaderProbe<ClusterStartOptions> probe, string survivor, string key, CancellationToken cancellationToken)
    {
        await probe.WaitForNoKnownLeaderAsync(survivor, Group, FailoverSteps.Bound, cancellationToken);
        await using var wire = WireClient.Connect(cluster.GetUri(survivor));
        using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        attempt.CancelAfter(AttemptTimeout);
        var refusal = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(wire.ReadAsync(CacheName, key, OperationDeadline, attempt.Token));

        _ = await Assert.That(refusal is RpcException { StatusCode: StatusCode.Unavailable } unavailable && string.Equals(unavailable.Status.Detail, WireClient.NoLeaderRefusalDetail, StringComparison.Ordinal))
            .IsTrue()
            .Because($"a node without a leader must refuse the read as unavailable within one attempt, got {Describe(refusal)}");
    }

    private static string Describe(Exception exception) => exception is RpcException rpc ? $"{nameof(RpcException)} {rpc.StatusCode} '{rpc.Status.Detail}'" : exception.GetType().Name;

    /// <summary>Determines whether a failure is one the client may answer by sending the write again: an unavailable or timed out call, or an unknown commit.</summary>
    /// <param name="exception">The failure.</param>
    /// <returns><see langword="true" /> for a retryable failure; otherwise <see langword="false" />.</returns>
    private static bool IsRetryable(Exception exception) =>
        exception is CommitOutcomeUnknownException or RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded };

    /// <summary>Stops the two other members, then writes through the survivor and measures how it fails.</summary>
    /// <param name="testName">The test name, which names the data directory and seeds the election jitter.</param>
    /// <param name="leaderSurvives">Whether the survivor is the leader of the group, or a follower.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    private static async Task WriteFailsAsync(string testName, bool leaderSurvives, CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (leader, _) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivor = leaderSurvives ? leader : FailoverSteps.Except(FailoverSteps.ThreeNodes, leader)[0];
        var key = FailoverSteps.KeysOf(CacheName, Group, "quorum", 1)[0];
        var cache = await cluster.GetCacheAsync<long>(CacheName, survivor, cancellationToken);
        await cache.SetAsync(key, 1L, cancellationToken: cancellationToken);

        foreach (var member in FailoverSteps.Except(FailoverSteps.ThreeNodes, survivor))
            await cluster.StopNodeAsync(member);

        // The token only guards against a hang: a call that outlasts it fails the test with a cancellation, not with a late failure.
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(FailoverSteps.Bound);
        var started = Stopwatch.GetTimestamp();
        var failure = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(cache.SetAsync(key, 2L, cancellationToken: guard.Token));
        var elapsed = Stopwatch.GetElapsedTime(started);

        if (!leaderSurvives)
            await AssertRefusesReadInOneAttemptAsync(cluster, probe, survivor, key, cancellationToken);

        _ = await Assert.That(leaderSurvives || failure is RpcException { StatusCode: StatusCode.Unavailable }).IsTrue().Because($"a follower without a leader must refuse the write as unavailable, got {Describe(failure)}");
        _ = await Assert.That(IsRetryable(failure)).IsTrue().Because($"a write without a quorum must fail retryably, got {Describe(failure)}");
        _ = await Assert.That(elapsed).IsLessThanOrEqualTo(OperationDeadline + Margin).Because($"the write ended after {elapsed.TotalSeconds:F1} s with {Describe(failure)}");
    }
}
