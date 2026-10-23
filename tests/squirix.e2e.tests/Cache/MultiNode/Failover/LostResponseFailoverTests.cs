using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// A mutation that committed on the leader, whose response never reached the client, keeps its first outcome across a leader change: the
/// client sends the same operation identifier through another node once a new leader is elected, and gets the first answer back without the
/// mutation running again.
/// </summary>
/// <remarks>
/// The entry node is a follower that forwards the write to the leader; the leader commits and answers, but the entry node never relays the answer
/// and refuses every further forward of the key. The leader then shuts down, and a later write changes the key as another client would: a
/// mutation that ran again would act on the later write, a replayed one leaves it untouched.
/// </remarks>
public sealed class LostResponseFailoverTests : EndToEndTestBase
{
    private const string CacheName = "lost-response";

    private const string First = "first";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    private const string Second = "second";

    /// <summary>The longest one call may take.</summary>
    private static readonly TimeSpan CallDeadline = TimeSpan.FromSeconds(30);

    /// <summary>A GetOrAdd that added its entry returns the same answer after the leader changed, and does not return the later write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public Task GetOrAddReplaysAfterLeaderChange(CancellationToken cancellationToken) =>
        ReplaysAsync(nameof(GetOrAddReplaysAfterLeaderChange), static key => WireMutation.GetOrAdd(CacheName, key, First), new WireOutcome(true, First), cancellationToken);

    /// <summary>A Set returns the same answer after the leader changed, and does not overwrite the later write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public Task SetReplaysAfterLeaderChange(CancellationToken cancellationToken) =>
        ReplaysAsync(nameof(SetReplaysAfterLeaderChange), static key => WireMutation.Set(CacheName, key, First), new WireOutcome(true, null), cancellationToken);

    /// <summary>
    /// Sends a mutation through a follower so that its response is lost after the leader committed it, shuts the leader down, lets the survivors
    /// elect a new one, writes the key again, and sends the same mutation through the other survivor.
    /// </summary>
    /// <param name="testName">The test name, which names the data directory and seeds the election jitter.</param>
    /// <param name="create">Builds the mutation for a key of the group.</param>
    /// <param name="replayed">The answer the first execution gave, which the second send must return.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    private static async Task ReplaysAsync(string testName, Func<string, WireMutation> create, WireOutcome replayed, CancellationToken cancellationToken)
    {
        var entryProbes = new EntryProbes();
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName, services: entryProbes.Configure), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var (entry, other) = (survivors[0], survivors[1]);
        var key = FailoverSteps.KeysOf(CacheName, Group, "lost", 1)[0];
        var mutation = create(key);
        entryProbes[entry].Lose(key, static () => Task.CompletedTask);

        await using var viaEntry = WireClient.Connect(cluster.GetUri(entry));
        var lost = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(viaEntry.SendAsync(mutation, CallDeadline, cancellationToken));
        var executions = (Owner: entryProbes[entry].OwnerExecutions(key), Forwards: entryProbes[entry].ForwardAttempts(key));
        await cluster.AbruptShutdownNodeAsync(former);
        await cluster.StopNodeAsync(former);
        var (_, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);

        var attempts = new List<EventualAttempt>();
        var cache = await cluster.GetCacheAsync<string>(CacheName, other, cancellationToken);
        await Eventually.SucceedsAsync((Cache: cache, Key: key), static (s, token) => s.Cache.SetAsync(s.Key, Second, cancellationToken: token), FailoverSteps.Bound, attempts, cancellationToken);
        await using var viaOther = WireClient.Connect(cluster.GetUri(other));
        var retried = await Eventually.SucceedsAsync((Client: viaOther, Mutation: mutation), static (s, token) => s.Client.SendAsync(s.Mutation, CallDeadline, token), FailoverSteps.Bound, attempts, cancellationToken);
        var read = await Eventually.SucceedsAsync((Cache: cache, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
        var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);

        var dump = Eventually.Dump(attempts);
        _ = await Assert.That(lost is RpcException { StatusCode: StatusCode.Unavailable }).IsTrue().Because($"the entry node must lose the response, got {lost.GetType().Name}: {lost.Message}");
        _ = await Assert.That(executions).IsEqualTo((1, 1));
        _ = await Assert.That(retried).IsEqualTo(replayed).Because(dump);
        _ = await Assert.That(read).IsEqualTo(new CacheValueResult<string>(true, Second)).Because(dump);
        _ = await Assert.That(report.ClientEntries).IsEqualTo(2);
        _ = await Assert.That(term).IsGreaterThan(formerTerm);
    }

    /// <summary>One lossy-forward probe per node of <see cref="FailoverSteps.ThreeNodes" />; the one on the entry node loses the response of the first forward of a key.</summary>
    private sealed class EntryProbes
    {
        private readonly LostForwardResponseProbe[] _probes = [new(), new(), new()];

        internal LostForwardResponseProbe this[string nodeId] => _probes[Array.IndexOf(FailoverSteps.ThreeNodes, nodeId)];

        internal void Configure(string nodeId, IServiceCollection services) => this[nodeId].Register(services);
    }
}
