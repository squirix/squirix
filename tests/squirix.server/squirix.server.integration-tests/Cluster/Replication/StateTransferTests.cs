using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>
/// A follower that was down while its group was written, and whose group log the other members compacted meanwhile, is repaired by a state
/// transfer once a member that holds the compacted log leads the group.
/// </summary>
/// <remarks>
/// Both tests build the same situation: three nodes with automatic failover and a low compaction threshold; one follower stops, the leader and the
/// other follower write the group, and the other follower compacts its log through them. The leader then stops and the stopped follower returns, so
/// the survivor wins the election and cannot catch the returned follower up from its log.
/// </remarks>
public sealed class StateTransferTests : NodeIntegrationTestBase
{
    private const string CacheName = "state-transfer";
    private const string GroupId = "node-a";
    private const string TransferCounter = "squirix_replication_state_transfers_total";
    private const int LaterKeys = 45;
    private const int FirstKeys = 5;

    /// <summary>The number of retained entries at which the nodes compact a group log.</summary>
    private const int Threshold = 16;

    /// <summary>Bounds every wait for the cluster; the waits only absorb a loaded machine.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(90);

    /// <summary>The maintenance interval of the nodes, far below the production default so a log is compacted within the bound.</summary>
    private static readonly TimeSpan MaintenanceInterval = TimeSpan.FromMilliseconds(100);

    private static readonly string[] Nodes = [GroupId, "node-b", "node-c"];

    /// <summary>Where a held transfer waits on the follower.</summary>
    private enum HoldPoint
    {
        /// <summary>Before the transfer enters the group.</summary>
        Enter = 0,

        /// <summary>Before the first batch of entries is written, after the keys of the group were removed.</summary>
        Write = 1,
    }

    /// <summary>
    /// The survivor leads and repairs the returned follower, which then holds every key with its value and an accounting that matches its entries;
    /// after the survivor stops, the repaired follower wins the election, because the third node lacks what it holds, and serves every key.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepairedFollowerHoldsEveryKeyAndLeads(CancellationToken cancellationToken)
    {
        const string scope = "state-transfer-leads";
        await using var fabric = new PartitionFabric();
        await using var lag = await LagAsync(scope, fabric, cancellationToken);
        await StartLaggingAsync(lag, null, cancellationToken);
        var (first, term) = await lag.Ledger.LeaderAsync([lag.Survivor, lag.Lagging], lag.Term, "the survivor leads the group", cancellationToken);
        await LedgerWrites.CommitAsync(lag.Ledger, lag.Cluster[first], CacheName, lag.Keys[^1], cancellationToken, ValueOf(lag.Keys.Length - 1));

        await AssertRepairedAsync(lag, cancellationToken);
        await fabric.PartitionAsync(lag.Survivor, lag.Former);
        _ = await lag.Cluster.StartNodeAsync(lag.Former, Options(scope, false, fabric, null), cancellationToken);
        await lag.Cluster.StopNodeAsync(lag.Survivor);
        var (second, _) = await lag.Ledger.LeaderAsync([lag.Lagging, lag.Former], term, "the repaired follower leads", cancellationToken);

        _ = await Assert.That(first).IsEqualTo(lag.Survivor);
        _ = await Assert.That(second).IsEqualTo(lag.Lagging);
        await AssertServesEveryKeyAsync(lag, cancellationToken);
    }

    /// <summary>
    /// The follower stops in the middle of a transfer, after its keys were wiped: it restarts marked incomplete, the leader does not admit it,
    /// and a new transfer ends with the right data.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InterruptedTransferIsRepeated(CancellationToken cancellationToken)
    {
        const string scope = "state-transfer-interrupted";
        await using var lag = await LagAsync(scope, null, cancellationToken);
        var midWrite = new Hold(HoldPoint.Write);
        await StartLaggingAsync(lag, midWrite.Configure, cancellationToken);
        await midWrite.Reached.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        await lag.Cluster[lag.Lagging].AbruptShutdownAsync();

        var beforeEnter = new Hold(HoldPoint.Enter);
        var restarted = await lag.Cluster.RestartNodeAsync(lag.Lagging, Options(scope, false, null, beforeEnter.Configure), cancellationToken);
        await beforeEnter.Reached.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        var (health, description) = await restarted.ReplicaGroupsAsync(cancellationToken);
        var admitted = lag.Cluster[lag.Survivor].GetRequiredService<ReplicaGroupRegistry>().EligibilityFor(GroupId).StateFor(SlotOf(restarted, lag.Lagging));
        var copy = (await Log(restarted).GetStatusAsync(cancellationToken)).MemberState;
        beforeEnter.Release();
        await LedgerWrites.CommitAsync(lag.Ledger, lag.Cluster[lag.Survivor], CacheName, lag.Keys[^1], cancellationToken, ValueOf(lag.Keys.Length - 1));

        _ = await Assert.That(copy).IsNotEqualTo(GroupMemberState.Complete);
        _ = await Assert.That(health).IsEqualTo(HealthStatus.Unhealthy);
        _ = await Assert.That(description).Contains("is incomplete on this node", StringComparison.Ordinal);
        _ = await Assert.That(admitted).IsNotEqualTo(ReplicaParticipantState.Ready);
        await AssertRepairedAsync(lag, cancellationToken);
    }

    private static string ValueOf(int index) => $"value-{index}";

    private static IFollowerLog Log(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(GroupId, out var log) ? log : throw new InvalidOperationException($"The node does not serve group log {GroupId}.");

    private static int SlotOf(ITestNodeHost host, string nodeId)
    {
        var locator = host.GetRequiredService<IReplicaGroupLocator>();
        var members = new string[locator.ReplicaCount];
        locator.GetReplicaGroup(GroupId, members);
        return Array.IndexOf(members, nodeId);
    }

    private static IntegrationStartOptions Options(string scope, bool clean, PartitionFabric? fabric, Action<IServiceCollection>? services) => new()
    {
        ReplicaCount = 3,
        UsePersistence = true,
        CleanTestDir = clean,
        ExtraScope = scope,
        AutomaticFailoverEnabled = true,
        QuorumReadsEnabled = true,
        ElectionTiming = WipedCluster.Timing,
        PartitionFabric = fabric,
        PersistenceOptions = new PersistenceOptions { JournalMaxSegmentMb = 64, ReplicaLogCompactionEntries = Threshold },
        ServicesConfigure = serviceCollection =>
        {
            SetMaintenanceInterval(serviceCollection);
            services?.Invoke(serviceCollection);
        },
    };

    /// <summary>Replaces the group log maintenance schedule.</summary>
    /// <param name="services">The node service collection.</param>
    private static void SetMaintenanceInterval(IServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == typeof(ReplicaLogCompactionOptions))
                services.RemoveAt(i);
        }

        _ = services.AddSingleton(new ReplicaLogCompactionOptions { Interval = MaintenanceInterval });
    }

    /// <summary>Finds keys of the group under test.</summary>
    /// <param name="host">A node of the cluster.</param>
    /// <param name="count">The number of keys.</param>
    /// <returns>Distinct keys the group owns.</returns>
    /// <exception cref="InvalidOperationException">The ring names too few keys of the group among the candidates.</exception>
    private static string[] KeysOf(ITestNodeHost host, int count)
    {
        var locator = host.GetRequiredService<INodeLocator>();
        var keys = new List<string>(count);
        for (var i = 0; keys.Count < count && i < 100_000; i++)
        {
            var candidate = $"{CacheName}-{i}";
            if (string.Equals(locator.GetOwner(CacheName, candidate), GroupId, StringComparison.Ordinal))
                keys.Add(candidate);
        }

        return keys.Count == count ? [.. keys] : throw new InvalidOperationException($"Only {keys.Count} of {count} keys of group {GroupId} were found.");
    }

    /// <summary>Writes keys of the group through the leader, each with the value that names its position.</summary>
    /// <param name="lag">The cluster.</param>
    /// <param name="range">The positions of the first key and after the last key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task CommitKeysAsync(Lag lag, (int From, int To) range, CancellationToken cancellationToken)
    {
        for (var i = range.From; i < range.To; i++)
            await LedgerWrites.CommitAsync(lag.Ledger, lag.Cluster[lag.Former], CacheName, lag.Keys[i], cancellationToken, ValueOf(i));
    }

    /// <summary>Stops the leader and returns the stopped follower, which the other follower then repairs once it leads.</summary>
    /// <param name="lag">The cluster.</param>
    /// <param name="services">A hook that registers services on the returned follower.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task StartLaggingAsync(Lag lag, Action<IServiceCollection>? services, CancellationToken cancellationToken)
    {
        await lag.Cluster.StopNodeAsync(lag.Former);
        _ = await lag.Cluster.StartNodeAsync(lag.Lagging, Options(lag.Scope, false, lag.Fabric, services), cancellationToken);
    }

    /// <summary>Sums the estimated size of the entries a node holds, as its memory accounting does.</summary>
    /// <param name="host">The node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The estimated bytes and the number of entries.</returns>
    private static async Task<(long Bytes, long Entries)> MeasureAsync(ITestNodeHost host, CancellationToken cancellationToken)
    {
        var estimator = host.GetRequiredService<ICacheEntrySizeEstimator<object?>>();
        long bytes = 0;
        long entries = 0;
        await foreach (var (key, entry) in host.GetRequiredService<ILocalCacheSnapshotReader<object?>>().EnumerateLiveAsync(cancellationToken))
        {
            bytes += estimator.EstimateBytes(key, entry, false);
            entries++;
        }

        return (bytes, entries);
    }

    /// <summary>Reads every key through the node that leads the group under quorum reads and checks its value.</summary>
    /// <param name="lag">The cluster, whose repaired follower leads.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private static async Task AssertServesEveryKeyAsync(Lag lag, CancellationToken cancellationToken)
    {
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(lag.Cluster[lag.Lagging], GroupId, [(lag.Former, lag.Cluster[lag.Former])], cancellationToken);
        for (var i = 0; i < lag.Keys.Length; i++)
        {
            var read = await FencedReads.ReadAsync(lag.Ledger, lag.Cluster[lag.Lagging], CacheName, lag.Keys[i], cancellationToken);
            _ = await Assert.That(read).IsEqualTo(ValueOf(i));
        }
    }

    /// <summary>Checks that the returned follower is complete, holds every key, accounts exactly for its entries, and that the leader counted the transfer.</summary>
    /// <param name="lag">The cluster, with the leader committed past the repair.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    private async Task AssertRepairedAsync(Lag lag, CancellationToken cancellationToken)
    {
        var repaired = lag.Cluster[lag.Lagging];
        await (Host: repaired, Key: lag.Keys[^1]).WaitUntilValueAsync(
            static async (state, token) => (await state.Host.GetRequiredService<PhysicalCache<object?>>().GetValueAsync(new CacheKey(CacheName, state.Key), token)).Found,
            Bound,
            cancellationToken);
        var cache = repaired.GetRequiredService<PhysicalCache<object?>>();
        var status = await Log(repaired).GetStatusAsync(cancellationToken);
        var metrics = await HttpClient.GetStringAsync(new Uri(lag.Cluster[lag.Survivor].Uri, "/metrics"), cancellationToken);
        var (bytes, entries) = await MeasureAsync(repaired, cancellationToken);
        var accounting = repaired.GetRequiredService<IMemoryUsageAccounting>();

        _ = await Assert.That(status.MemberState).IsEqualTo(GroupMemberState.Complete);
        _ = await Assert.That(metrics).Contains(TransferCounter, StringComparison.Ordinal);
        _ = await Assert.That(metrics).Contains("outcome=\"transferred\"", StringComparison.Ordinal);
        _ = await Assert.That(entries).IsEqualTo(lag.Keys.Length);
        _ = await Assert.That((accounting.ReadEstimatedBytes(), accounting.ReadEntryCount())).IsEqualTo((bytes, entries));
        for (var i = 0; i < lag.Keys.Length; i++)
        {
            var read = await cache.GetValueAsync(new CacheKey(CacheName, lag.Keys[i]), cancellationToken);
            _ = await Assert.That(read.Found).IsTrue();
            _ = await Assert.That(read.Value).IsEqualTo(ValueOf(i));
        }
    }

    /// <summary>
    /// Starts the cluster, writes the first keys, stops a follower, writes the later keys without it, and waits until the other follower has compacted its
    /// group log past everything the stopped follower holds.
    /// </summary>
    /// <param name="scope">The scope of the data directories.</param>
    /// <param name="fabric">The fabric the nodes dial each other through, if the test cuts links.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The cluster in that state.</returns>
    private async Task<Lag> LagAsync(string scope, PartitionFabric? fabric, CancellationToken cancellationToken)
    {
        var cluster = await StartClusterAsync(Nodes[0], Nodes[1], Nodes[2], Options(scope, true, fabric, null), cancellationToken, scope);
        try
        {
            var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, GroupId, Bound);
            var (former, term) = await ledger.LeaderAsync(Nodes, 0UL, "the group gets a leader", cancellationToken);
            var lagging = string.Equals(former, Nodes[1], StringComparison.Ordinal) ? Nodes[2] : Nodes[1];
            var survivor = Array.Find(Nodes, id => !string.Equals(id, former, StringComparison.Ordinal) && !string.Equals(id, lagging, StringComparison.Ordinal));
            var lag = new Lag(cluster, ledger, scope, fabric)
            {
                Former = former,
                Lagging = lagging,
                Survivor = survivor!,
                Keys = KeysOf(cluster[former], FirstKeys + LaterKeys + 1),
                Term = term,
            };
            await CommitKeysAsync(lag, (0, FirstKeys), cancellationToken);
            await ReplicaGroupFollowers.AwaitCaughtUpAsync(cluster[former], GroupId, [(lagging, cluster[lagging]), (lag.Survivor, cluster[lag.Survivor])], cancellationToken);
            var held = (await ReplicaLogReads.ReadAsync(cluster, lagging, GroupId, cancellationToken)).LastLogIndex;
            await cluster.StopNodeAsync(lagging);
            await CommitKeysAsync(lag, (FirstKeys, FirstKeys + LaterKeys), cancellationToken);
            await ReplicaGroupFollowers.AwaitCaughtUpAsync(cluster[former], GroupId, [(lag.Survivor, cluster[lag.Survivor])], cancellationToken);
            await (Cluster: cluster, Node: lag.Survivor, Held: held).WaitUntilValueAsync(
                static async (state, token) => (await ReplicaLogReads.ReadAsync(state.Cluster, state.Node, GroupId, token)).SnapshotIndex > state.Held,
                Bound,
                cancellationToken);
            return lag;
        }
        catch
        {
            await cluster.DisposeAsync();
            throw;
        }
    }

    /// <summary>A cluster after the writes and the compaction, before the leader stops.</summary>
    private sealed class Lag : IAsyncDisposable
    {
        internal Lag(TestCluster<IntegrationStartOptions> cluster, GroupAuthorityLedger<IntegrationStartOptions> ledger, string scope, PartitionFabric? fabric)
        {
            Cluster = cluster;
            Ledger = ledger;
            Scope = scope;
            Fabric = fabric;
        }

        internal required string Former { get; init; }

        internal required string Lagging { get; init; }

        internal required string Survivor { get; init; }

        internal required string[] Keys { get; init; }

        internal required ulong Term { get; init; }

        internal TestCluster<IntegrationStartOptions> Cluster { get; }

        internal PartitionFabric? Fabric { get; }

        internal GroupAuthorityLedger<IntegrationStartOptions> Ledger { get; }

        internal string Scope { get; }

        public ValueTask DisposeAsync() => Cluster.DisposeAsync();
    }

    /// <summary>Holds the first call of a transfer at one point on the follower until the test releases it.</summary>
    private sealed class Hold
    {
        private readonly HoldPoint _point;
        private readonly TaskCompletionSource _reached = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _taken;

        internal Hold(HoldPoint point)
        {
            _point = point;
        }

        internal Task Reached => _reached.Task;

        /// <summary>Wraps the state sink of the node so the transfer waits at the point.</summary>
        /// <param name="services">The node service collection.</param>
        /// <exception cref="InvalidOperationException">The node registers no state sink factory.</exception>
        internal void Configure(IServiceCollection services)
        {
            var index = -1;
            for (var i = services.Count - 1; i >= 0 && index < 0; i--)
            {
                if (services[i].ServiceType == typeof(IReplicaStateSink) && services[i].ImplementationFactory != null)
                    index = i;
            }

            if (index < 0)
                throw new InvalidOperationException("The node registers no state sink factory.");

            var factory = services[index].ImplementationFactory!;
            services[index] = ServiceDescriptor.Singleton<IReplicaStateSink>(provider => Wrap(factory(provider)));
        }

        internal void Release() => _ = _release.TrySetResult();

        private HeldSink Wrap(object inner) =>
            inner is IReplicaStateSink sink ? new HeldSink(sink, this) : throw new InvalidOperationException("The registered state sink is not a state sink.");

        private async ValueTask WaitAsync(HoldPoint point, CancellationToken cancellationToken)
        {
            if (point != _point || Interlocked.Exchange(ref _taken, 1) == 1)
                return;

            _ = _reached.TrySetResult();
            await _release.Task.WaitAsync(cancellationToken);
        }

        private sealed class HeldSink : IReplicaStateSink
        {
            private readonly Hold _hold;
            private readonly IReplicaStateSink _inner;

            internal HeldSink(IReplicaStateSink inner, Hold hold)
            {
                _inner = inner;
                _hold = hold;
            }

            public async ValueTask<bool> EnterAsync(string groupId, CancellationToken cancellationToken)
            {
                await _hold.WaitAsync(HoldPoint.Enter, cancellationToken);
                return await _inner.EnterAsync(groupId, cancellationToken);
            }

            public ValueTask<bool> WipeStepAsync(string groupId, int maxExamined, CancellationToken cancellationToken) => _inner.WipeStepAsync(groupId, maxExamined, cancellationToken);

            public async ValueTask WriteAsync(string groupId, ReadOnlyMemory<ReplicaStateEntry> entries, CancellationToken cancellationToken)
            {
                await _hold.WaitAsync(HoldPoint.Write, cancellationToken);
                await _inner.WriteAsync(groupId, entries, cancellationToken);
            }

            public ValueTask<ReplicaStateTotals> SealAsync(string groupId, CancellationToken cancellationToken) => _inner.SealAsync(groupId, cancellationToken);

            public void ResetAppliedTo(string groupId, ulong index) => _inner.ResetAppliedTo(groupId, index);

            public void Leave(string groupId) => _inner.Leave(groupId);
        }
    }
}
