using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>A new leader gains authority without waiting for a follower that is gone.</summary>
/// <remarks>
/// The survivors are watched through the route signal of their election state, so the time from a term first seen to the authority
/// of that term is measured without polling.
/// </remarks>
public sealed class PromotionLatencyTests : NodeIntegrationTestBase
{
    private const string Group = "node-a";
    private const string Scope = "promotion-latency";

    /// <summary>Bounds every wait; the timing below elects within seconds, the rest absorbs a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    private static readonly ElectionTimerOptions Timing = new()
    {
        ElectionTimeout = TimeSpan.FromSeconds(2),
        HeartbeatInterval = TimeSpan.FromMilliseconds(100),
        MaxJitter = TimeSpan.FromSeconds(1),
        VoteRpcTimeout = TimeSpan.FromMilliseconds(200),
    };

    private static readonly string[] Three = ["node-a", "node-b", "node-c"];

    /// <summary>
    /// With the stopped leader black-holed, so every dial to it hangs, the new leader gains authority within one vote timeout and one
    /// probe timeout of its term being first seen, with its leader-term entry committed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoppedLeaderDoesNotDelayPromotion(CancellationToken cancellationToken)
    {
        await using var fabric = new PartitionFabric();
        var topology = new ClusterNode[Three.Length];
        for (var i = 0; i < topology.Length; i++)
            topology[i] = new ClusterNode(Three[i], GetNextHttpUri());

        await using var cluster = await StartClusterAsync(topology, Options(fabric), cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, Group, Bound);
        var (former, formerTerm) = await ledger.LeaderAsync(Three, 0UL, "the group gets a leader", cancellationToken);
        var survivors = Array.FindAll(Three, id => !string.Equals(id, former, StringComparison.Ordinal));
        await using var watch = new RouteWatch([cluster[survivors[0]], cluster[survivors[1]]], formerTerm);

        await fabric.BlackHoleAsync(former);
        await cluster.StopNodeAsync(former);
        var (next, nextTerm) = await ledger.LeaderAsync(survivors, formerTerm, "a survivor gains authority", cancellationToken);
        var authorized = await watch.AuthorizedAsync.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        var (noop, committed) = await NoopAsync(cluster[next], nextTerm, cancellationToken);
        var promotion = watch.PromotionTime(authorized);
        var allowed = Timing.VoteRpcTimeout + ReplicaVerificationProbe.ProbeTimeout;

        _ = await Assert.That(authorized).IsEqualTo(nextTerm);
        _ = await Assert.That((noop.MutationKind, noop.Term, committed)).IsEqualTo((ReplicaMutationKinds.LeaderNoop, nextTerm, true));
        _ = await Assert.That(promotion).IsLessThan(allowed).Because($"term {nextTerm} took {promotion.TotalMilliseconds:F0} ms from first seen to authority, allowed {allowed.TotalMilliseconds:F0} ms");
    }

    /// <summary>Reads the leader-term entry the leader appended in its term, and whether it is committed.</summary>
    /// <param name="leader">The leader.</param>
    /// <param name="term">The term the leader holds authority in.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The decoded record and whether the commit index reaches it.</returns>
    /// <exception cref="InvalidOperationException">The leader holds no leadership of the term, or its log holds no decodable entry at the index.</exception>
    private static async Task<(ReplicaLogRecord Record, bool Committed)> NoopAsync(ITestNodeHost leader, ulong term, CancellationToken cancellationToken)
    {
        var committers = leader.GetRequiredService<ReplicaGroupCommitters>();
        var tenure = committers.Leads(Group) ? committers.For(Group).Tenure : throw new InvalidOperationException($"The leader no longer leads group {Group}.");
        var index = tenure is { } held && held.Term == term ? held.NoopIndex : throw new InvalidOperationException($"The leader holds no leadership of term {term}.");
        var log = leader.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(Group, out var opened) ? opened : throw new InvalidOperationException($"The group log {Group} is not open.");
        var status = await log.GetStatusAsync(cancellationToken);
        var read = await log.ReadEntriesAsync(index, 1, cancellationToken);
        var record = read.Entries.Count == 1 ? ReplicaLogCodec.Decode(read.Entries[0].Payload) : null;
        return record is { } noop ? (noop, status.CommitIndex >= index)
            : throw new InvalidOperationException($"The group log {Group} holds no decodable entry at {index}.");
    }

    private static IntegrationStartOptions Options(PartitionFabric fabric) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        ExtraScope = Scope,
        AutomaticFailoverEnabled = true,
        QuorumReadsEnabled = true,
        PartitionFabric = fabric,
        ServicesConfigure = static services => services.AddSingleton(Timing),
    };

    /// <summary>Records when the election state of each watched node first showed each term, and when it held authority in a term above a given one.</summary>
    private sealed class RouteWatch : IAsyncDisposable
    {
        private readonly TaskCompletionSource<ulong> _authorized = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly ConcurrentDictionary<ulong, long> _authorityAt = new();
        private readonly ConcurrentDictionary<ulong, long> _firstSeen = new();
        private readonly Task[] _loops;
        private readonly CancellationTokenSource _stop = new();

        internal RouteWatch(ITestNodeHost[] nodes, ulong above)
        {
            _loops = new Task[nodes.Length];
            for (var i = 0; i < nodes.Length; i++)
                _loops[i] = WatchAsync(nodes[i].GetRequiredService<ReplicaGroupRegistry>().StateFor(Group), above);
        }

        /// <summary>Gets the first term above the given one in which a watched node held authority, once it did.</summary>
        internal Task<ulong> AuthorizedAsync => _authorized.Task;

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            await Task.WhenAll(_loops);
            _stop.Dispose();
        }

        /// <summary>Returns the time from the first sight of a term on any watched node to the authority held in it.</summary>
        /// <param name="term">The term.</param>
        /// <returns>The elapsed time.</returns>
        internal TimeSpan PromotionTime(ulong term) => Stopwatch.GetElapsedTime(_firstSeen[term], _authorityAt[term]);

        private async Task WatchAsync(ReplicaGroupState state, ulong above)
        {
            try
            {
                while (true)
                {
                    // The version is read before the view, so a change published between the read and the wait ends the wait at once.
                    var version = state.RouteChanged.Version;
                    var view = state.ReadRoute();
                    var now = Stopwatch.GetTimestamp();
                    _ = _firstSeen.TryAdd(view.Term, now);
                    if (view.HasAuthority && view.Term > above && _authorityAt.TryAdd(view.Term, now))
                        _ = _authorized.TrySetResult(view.Term);

                    _ = await state.RouteChanged.WaitAsync(version, Timeout.InfiniteTimeSpan, TimeProvider.System, _stop.Token);
                }
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                // The watch ended with the test.
            }
        }
    }
}
