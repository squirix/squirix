using System;
using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The memory a node accounts includes the copies it holds as a follower of an RF=3 group, and committed entries never wait for memory.</summary>
public sealed class ReplicaFollowerMemoryTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";
    private const string GroupId = "n1";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private string OwnerDir => Path.Join(Dir, "owner");

    /// <summary>Each follower that applied the committed entries of the group accounts exactly the entries it holds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerAccountsTheEntriesItApplied(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        await CommitAsync(committer, 5, cancellationToken);
        await gateway.AppendedAsync("n2", 5).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await gateway.AppendedAsync("n3", 5).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        var estimator = new CacheEntrySizeEstimator<object?>();

        foreach (var node in new[] { "n2", "n3" })
        {
            var copy = new FollowerCopy(node, estimator);
            await copy.CatchUpAsync(followers.Logs[node], cancellationToken);

            _ = await Assert.That(copy.Applier.AppliedIndex).IsGreaterThan(0UL);
            _ = await Assert.That(Convert.ToUInt64(CountOf(copy.Cache))).IsEqualTo(copy.Applier.AppliedIndex);
            _ = await Assert.That(BytesOf(copy.Cache)).IsEqualTo(await SumOfLiveEntriesAsync(copy.Cache, estimator, cancellationToken));
            _ = await Assert.That(BytesOf(copy.Cache)).IsGreaterThan(0L);
        }
    }

    /// <summary>
    /// A node whose memory the copies of followed groups fill refuses a write that grows memory in a group it leads, still applies the entries
    /// that group committed since, and still admits a remove.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FilledNodeRefusesGrowthStillApplies(CancellationToken cancellationToken)
    {
        using var meter = new Meter("squirix-follower-memory");
        await using var followers = await OpenFollowersAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(followers.Logs);
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var committer = CreateCommitter(registry, gateway);
        var estimator = new CacheEntrySizeEstimator<object?>();
        var copy = new FollowerCopy("n2", estimator);
        await CommitAsync(committer, 3, cancellationToken);
        await gateway.AppendedAsync("n2", 3).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await copy.CatchUpAsync(followers.Logs["n2"], cancellationToken);
        ILocalCacheStats stats = copy.Cache;
        var filledBytes = stats.EstimatedBytes;
        var filledEntries = stats.EntryCount;
        var options = Options.Create(new PressureOptions { MaxEstimatedCacheBytes = filledBytes });
        var gateOfNode = new PressureGate(new StateEvaluator(options.Value), new MemoryUsageAccounting(copy.Cache), "n2", meter);
        var admission = new MemoryAdmissionCacheDecorator<object?>(copy.Local, gateOfNode, estimator);

        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(SetLedKeyAsync(admission, cancellationToken));
        await CommitAsync(committer, 3, cancellationToken, 4);
        await gateway.AppendedAsync("n2", 6).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await copy.CatchUpAsync(followers.Logs["n2"], cancellationToken);
        var appliedBytes = stats.EstimatedBytes;
        var removed = await admission.RemoveAsync(NewOperationId(), CacheName, "k1", cancellationToken);

        _ = await Assert.That(filledEntries).IsGreaterThan(0);
        _ = await Assert.That(stats.EntryCount).IsGreaterThan(filledEntries);
        _ = await Assert.That(appliedBytes).IsGreaterThan(filledBytes);
        _ = await Assert.That(removed.Removed).IsTrue();
        _ = await Assert.That(stats.EstimatedBytes).IsLessThan(appliedBytes);
        _ = await NodeAsyncAssert.ThrowsAsync<ResourceExhaustedException>(SetLedKeyAsync(admission, cancellationToken));
    }

    private static async Task CommitAsync(ReplicaGroupCommitter committer, int count, CancellationToken cancellationToken, int first = 1)
    {
        for (var i = first; i < first + count; i++)
        {
            var key = $"k{i}";
            await committer.CommitSetAsync(NewOperationId(), CacheName, key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
    }

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static int CountOf(ILocalCacheStats stats) => stats.EntryCount;

    private static Task SetLedKeyAsync(MemoryAdmissionCacheDecorator<object?> admission, CancellationToken cancellationToken) =>
        admission.SetEntryAsync(NewOperationId(), CacheName, "led", Entry("led"), cancellationToken).AsTask();

    private static async Task<long> SumOfLiveEntriesAsync(PhysicalCache<object?> cache, CacheEntrySizeEstimator<object?> estimator, CancellationToken cancellationToken)
    {
        long sum = 0;
        await foreach (var (key, entry) in cache.EnumerateLiveAsync(cancellationToken))
            sum += estimator.EstimateBytes(key, entry, false);

        return sum;
    }

    private async Task<Followers> OpenFollowersAsync(CancellationToken cancellationToken)
    {
        var followers = new Followers();
        try
        {
            foreach (var node in new[] { "n2", "n3" })
            {
                var log = new FollowerLog(Path.Join(Dir, node), GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
                followers.Logs[node] = log;
                await log.OpenSeededAsync(cancellationToken);
            }
        }
        catch
        {
            await followers.DisposeAsync();
            throw;
        }

        return followers;
    }

    /// <summary>The follower logs of the group, disposed together.</summary>
    private sealed class Followers : IAsyncDisposable
    {
        internal ConcurrentDictionary<string, FollowerLog> Logs { get; } = new(StringComparer.Ordinal);

        public async ValueTask DisposeAsync()
        {
            foreach (var (_, log) in Logs)
                await log.DisposeAsync();
        }
    }

    /// <summary>The memory of a follower node and the applier that fills it with the committed entries of the group.</summary>
    private sealed class FollowerCopy
    {
        internal FollowerCopy(string node, CacheEntrySizeEstimator<object?> estimator)
        {
            Cache = new PhysicalCache<object?>(TimeProvider.System, estimator);
            Local = new ClientCache<object?>(Cache, Cache);
            Applier = new ReplicaGroupApplier(Local, NullLogger.Instance, GroupId, node);
        }

        internal ReplicaGroupApplier Applier { get; }

        internal PhysicalCache<object?> Cache { get; }

        internal ClientCache<object?> Local { get; }

        /// <summary>Applies the entries the follower log committed since the last call.</summary>
        /// <param name="log">The follower log of the node.</param>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>An asynchronous operation.</returns>
        internal async Task CatchUpAsync(FollowerLog log, CancellationToken cancellationToken)
        {
            await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
            var status = await log.GetStatusAsync(cancellationToken);
            await Applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, cancellationToken);
        }
    }
}
