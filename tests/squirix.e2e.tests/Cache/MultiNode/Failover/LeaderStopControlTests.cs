using System;
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

/// <summary>
/// The negative control of the leader stop tests: the same RF=3 scene without automatic failover. When the ring owner of a group stops, no
/// survivor takes authority over the group and no node sees a later term, every write and every read of the group through a survivor is
/// refused for the whole recovery bound, and a group a survivor owns keeps serving. A leader stop test that meets the bound therefore shows
/// that failover made the group available again, not that an unaffected path stayed healthy.
/// </summary>
public sealed class LeaderStopControlTests : EndToEndTestBase
{
    private const string CacheName = "leader-stop-control";

    /// <summary>The group under test, named after its owner, which leads it statically without failover.</summary>
    private const string Group = "nodeA";

    /// <summary>The value the probe key holds before the fault, written through a survivor while the owner runs.</summary>
    private const long SeedValue = 1L;

    /// <summary>The value the probe tries to write after the fault, above the seed.</summary>
    private const long ProbeValue = 2L;

    /// <summary>The term a ring owner leads its group in when no election runs.</summary>
    private const ulong StaticTerm = 1UL;

    /// <summary>The number of terms in which some node held authority over the group when no election ever ran: the static term alone.</summary>
    private const int TermsWithoutElection = 1;

    /// <summary>The pause between two probe attempts, so an attempt refused at once does not spin.</summary>
    private static readonly TimeSpan Backoff = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// The owner of the group stops while failover is off: no node takes the group over within the recovery bound, every write and read of
    /// the group through a survivor is refused meanwhile, and the survivors still serve their own groups.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task LeaderStopWithoutFailoverMissesBound(CancellationToken cancellationToken)
    {
        const string testName = nameof(LeaderStopWithoutFailoverMissesBound);

        // The scene of the leader stop tests with failover off: the ring owner leads every group statically and no node ever takes over.
        var options = new MultiNodeStartOptions { ReplicaCount = FailoverSteps.ThreeNodes.Length, Failover = false, ElectionTiming = FailoverTiming.For(testName) };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, options, true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (owner, ownerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        _ = await Assert.That(owner).IsEqualTo(Group);
        _ = await Assert.That(ownerTerm).IsEqualTo(StaticTerm);

        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, owner);
        var cache = await (await cluster.ConnectClientAsync(survivors[0], cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var key = FailoverSteps.KeysOf(CacheName, Group, "control", 1)[0];
        var survivorKey = FailoverSteps.KeysOf(CacheName, survivors[0], "survivor", 1)[0];
        await cache.SetAsync(key, SeedValue, cancellationToken: cancellationToken);

        await cluster.StopNodeAsync(owner);
        var attempts = new List<EventualAttempt>();
        var probing = ProbeAsync(cache, key, attempts, cancellationToken);
        var watching = probe.AssertNoElectionAsync(Group, FailoverSteps.RecoveryBound, false, cancellationToken);

        // The probe ends at the bound on its own, so a failed watch still waits for it before the cluster is disposed.
        await Task.WhenAll(watching, probing);
        var (writesAcked, readsServed) = await probing;
        await cache.SetAsync(survivorKey, SeedValue, cancellationToken: cancellationToken);
        var survivorRead = await cache.GetValueAsync(survivorKey, cancellationToken);

        var dump = Eventually.Dump(attempts);
        _ = await Assert.That(writesAcked).IsEqualTo(0).Because(dump);
        _ = await Assert.That(readsServed).IsEqualTo(0).Because(dump);
        _ = await Assert.That(attempts).IsNotEmpty().Because(dump);
        _ = await Assert.That(probe.Ledger(Group).Terms).IsEqualTo(TermsWithoutElection).Because(dump);
        _ = await Assert.That(survivorRead).IsEqualTo(new CacheValueResult<long>(true, SeedValue));
    }

    /// <summary>
    /// Writes the probe value and, independently, reads the key through a survivor, again and again until the recovery bound is spent. A
    /// refusal the cluster gives while the group has no leader counts as one attempt; any other failure ends the probe, since it would not
    /// be the outage under test. A call that hangs ends at the bound.
    /// </summary>
    /// <param name="cache">The cache to probe through; its node must survive the fault.</param>
    /// <param name="key">The probe key, of the group that lost its owner.</param>
    /// <param name="attempts">Receives one entry per attempt, naming the outcome of the write and of the read.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>How many writes were acknowledged and how many reads were served within the bound.</returns>
    private static async Task<(int WritesAcked, int ReadsServed)> ProbeAsync(ICache<long> cache, string key, List<EventualAttempt> attempts, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        var writesAcked = 0;
        var readsServed = 0;
        using var limit = new CancellationTokenSource(FailoverSteps.RecoveryBound);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, limit.Token);
        while (!linked.IsCancellationRequested)
        {
            var attemptStarted = Stopwatch.GetElapsedTime(started);
            string write;
            try
            {
                await cache.SetAsync(key, ProbeValue, cancellationToken: linked.Token);
                writesAcked++;
                write = "write acked";
            }
            catch (Exception exception) when (IsRefusal(exception, limit))
            {
                write = $"write {Describe(exception)}";
            }

            string read;
            try
            {
                var result = await cache.GetValueAsync(key, linked.Token);
                readsServed++;
                read = $"read {result.Value}";
            }
            catch (Exception exception) when (IsRefusal(exception, limit))
            {
                read = $"read {Describe(exception)}";
            }

            attempts.Add(new EventualAttempt(attemptStarted, Stopwatch.GetElapsedTime(started) - attemptStarted, $"{write}, {read}"));
            await Task.Delay(Backoff, TimeProvider.System, linked.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return (writesAcked, readsServed);
    }

    /// <summary>
    /// Tells whether a failure is one the cluster gives while the group has no leader, or the bound cutting a call short: the same failures
    /// the recovery probe of the leader stop tests retries.
    /// </summary>
    /// <param name="exception">The failure.</param>
    /// <param name="limit">The source canceled at the bound.</param>
    /// <returns><see langword="true" /> when the failure counts as a refused attempt.</returns>
    private static bool IsRefusal(Exception exception, CancellationTokenSource limit) =>
        exception is RpcException { StatusCode: StatusCode.Unavailable or StatusCode.DeadlineExceeded } or CommitOutcomeUnknownException
        || (exception is OperationCanceledException && limit.IsCancellationRequested);

    /// <summary>Names a failed call by its status code, or by its exception type when it is not a gRPC failure.</summary>
    /// <param name="exception">The failure.</param>
    /// <returns>The name.</returns>
    private static string Describe(Exception exception) => exception is RpcException rpc ? $"{nameof(RpcException)} {rpc.StatusCode}" : exception.GetType().Name;
}
