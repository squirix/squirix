using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// A write whose outcome the client does not know because its leader died keeps one logical identity: sent again with the same operation
/// identifier through a surviving member once a new leader is elected, it takes effect once, and sending it yet again returns the same answer.
/// </summary>
public sealed class UnknownCommitFailoverTests : EndToEndTestBase
{
    private const string CacheName = "unknown-commit";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    private const string Value = "first";

    /// <summary>The longest one call may take; the leader dies long before it elapses.</summary>
    private static readonly TimeSpan CallDeadline = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The leader appends a TryAdd that only one follower receives and shuts down abruptly with the client still waiting; the new leader commits the
    /// entry of the previous term. The client sends the same TryAdd again through a survivor: it replays the first outcome, a further
    /// send returns the same answer, and the committed log holds it once.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task RetryAfterLeaderDeathTakesEffectOnce(CancellationToken cancellationToken)
    {
        const string testName = nameof(RetryAfterLeaderDeathTakesEffectOnce);
        await using var fabric = new PartitionFabric();
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName, fabric: fabric), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var key = FailoverSteps.KeysOf(CacheName, Group, "unknown", 1)[0];
        var mutation = WireMutation.AddIfAbsent(CacheName, key, Value);
        var appended = await GroupLogReads.LastIndexAsync(cluster.Cluster, former, Group, cancellationToken);

        // The first follower receives the append but its acknowledgements never reach the leader, and the leader cannot reach the second follower,
        // so the write stays uncommitted for the leader and the client, while the first follower alone holds the entry of the previous term.
        var holder = survivors[0];
        fabric.HoldDirection(holder, former);
        fabric.HoldDirection(former, survivors[1]);

        await using var viaLeader = WireClient.Connect(cluster.GetUri(former));
        var first = viaLeader.SendAsync(mutation, CallDeadline, cancellationToken);
        await probe.Ledger(Group).UntilValueAsync(
            (cluster.Cluster, Node: holder, Before: appended),
            static async (s, token) => await GroupLogReads.LastIndexAsync(s.Cluster, s.Node, Group, token) > s.Before,
            "a follower holds the appended write while the leader cannot learn it committed",
            cancellationToken);
        var held = await GroupLogReads.LastIndexAsync(cluster.Cluster, holder, Group, cancellationToken);
        _ = await Assert.That(held).IsGreaterThan(appended).Because("the entry must reach a follower before the leader dies, or the retry is a plain first execution");
        await cluster.AbruptShutdownNodeAsync(former);
        await cluster.StopNodeAsync(former);
        var unknown = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(first);
        fabric.HealAll();

        var (_, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
        await using var viaSurvivor = WireClient.Connect(cluster.GetUri(survivors[0]));
        var attempts = new List<EventualAttempt>();
        var retried = await Eventually.SucceedsAsync((Client: viaSurvivor, Mutation: mutation), static (s, token) => s.Client.SendAsync(s.Mutation, CallDeadline, token), FailoverSteps.Bound, attempts, cancellationToken);
        var repeated = await Eventually.SucceedsAsync((Client: viaSurvivor, Mutation: mutation), static (s, token) => s.Client.SendAsync(s.Mutation, CallDeadline, token), FailoverSteps.Bound, attempts, cancellationToken);
        var cache = await cluster.GetCacheAsync<string>(CacheName, survivors[1], cancellationToken);
        var read = await Eventually.SucceedsAsync((Cache: cache, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);

        var dump = Eventually.Dump(attempts);
        _ = await Assert.That(IsRetryable(unknown)).IsTrue().Because($"the client of the dead leader must see a retryable failure, got {unknown.GetType().Name}: {unknown.Message}");
        _ = await Assert.That(retried).IsEqualTo(new WireOutcome(true, null)).Because(dump);
        _ = await Assert.That(repeated).IsEqualTo(retried).Because(dump);
        _ = await Assert.That(read).IsEqualTo(new CacheValueResult<string>(true, Value)).Because(dump);
        _ = await Assert.That(report.ClientEntries).IsEqualTo(1);
        _ = await Assert.That(term).IsGreaterThan(formerTerm);
    }

    /// <summary>Determines whether a failure is one the client may answer by sending the same operation again.</summary>
    /// <param name="exception">The failure.</param>
    /// <returns><see langword="true" /> for a retryable status or an unknown commit outcome; otherwise <see langword="false" />.</returns>
    private static bool IsRetryable(Exception exception) =>
        exception is CommitOutcomeUnknownException or RpcException { StatusCode: StatusCode.Unavailable or StatusCode.Internal or StatusCode.DeadlineExceeded or StatusCode.Cancelled };
}
