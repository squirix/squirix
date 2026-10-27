using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Value reads of the replicated cache on the owner of group n1, which it leads statically: a hit on a live entry completes synchronously;
/// an expired entry is still expired on the leader before the read reports.
/// </summary>
public sealed class ReplicatedCacheValueReadTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";

    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    /// <summary>A value read with the deadline, which the replicated cache answers from its entry read, carries the value and the deadline of a live entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeadlineReadCarriesEntryDeadline(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-value-read-deadline");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), local, clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        ILogicalNamespacedCache<object?> cache = new ReplicatedCache(local, LeadOwn(committer));

        var read = await cache.GetValueWithDeadlineAsync(CacheName, Key, cancellationToken);
        var miss = await cache.GetValueWithDeadlineAsync(CacheName, "absent", cancellationToken);

        _ = await Assert.That(read).IsEqualTo(new NodeCacheValueRead<object?>(true, "v", Start.UtcDateTime + Ttl));
        _ = await Assert.That(miss).IsEqualTo(default);
    }

    /// <summary>A value read of an expired entry commits its tombstone on the leader and misses, as an entry read does.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredValueReadCommitsTombstone(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-value-read-expired");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), local, clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var cache = new ReplicatedCache(local, LeadOwn(committer));
        _ = await cache.GetValueAsync(CacheName, Key, cancellationToken);
        clock.Advance(Ttl);

        var read = await cache.GetValueAsync(CacheName, Key, cancellationToken);

        _ = await Assert.That(read.Found).IsFalse();
        _ = await Assert.That(await LastKindAsync(registry, cancellationToken)).IsEqualTo(ReplicaMutationKinds.Expire);
        _ = await Assert.That(await physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>
    /// A value read that finds its entry expired while a write lands before the leader decides the expiry reports the live value the leader
    /// finds, and commits no tombstone.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredValueReadReturnsLeaderLiveValue(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-value-read-revived");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), local, clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        clock.Advance(Ttl);

        // The read sees the expired entry, then a write of a new live value commits before the read asks the leader to expire it.
        var inner = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = inner.Setups.GetValueWithDeadlineAsync(CacheName, Key, Arg.Any<CancellationToken>()).Callback(
            Bind.To<(ClientCache<object?> Local, ReplicaGroupCommitter Committer), Func<string, string, CancellationToken, ValueTask<NodeCacheValueRead<object?>>>>(
                (local, committer),
                static s => async (cacheName, key, token) =>
                {
                    var stale = await s.Local.GetValueWithDeadlineAsync(cacheName, key, token);
                    await s.Committer.CommitSetAsync(NewOperationId(), cacheName, key, new NodeCacheEntry<object?>("w", 1, null, Ttl), token);
                    return stale;
                }));
        var cache = new ReplicatedCache(inner.Instance(), LeadOwn(committer));

        var read = await cache.GetValueAsync(CacheName, Key, cancellationToken);

        _ = await Assert.That(read).IsEqualTo(new NodeCacheValueResult<object?>(true, "w"));
        _ = await Assert.That(await LastKindAsync(registry, cancellationToken)).IsEqualTo(ReplicaMutationKinds.Set);
    }

    /// <summary>A hit on a live entry completes synchronously with the value; its allocations are measured by ReplicatedCacheValueReadBenchmarks.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LiveHitCompletesSynchronously(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-value-read-live");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), local, clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var cache = new ReplicatedCache(local, LeadOwn(committer));

        // The first read checks the seeding of the copy.
        _ = await cache.GetValueAsync(CacheName, Key, cancellationToken);
        var read = cache.GetValueAsync(CacheName, Key, cancellationToken);

        _ = await Assert.That(read.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(await read).IsEqualTo(new NodeCacheValueResult<object?>(true, "v"));
    }

    /// <summary>
    /// A read whose expiry the leader answers with an entry the reader's clock already finds past its deadline is refused retryably instead
    /// of returning the stale entry.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LiveAnswerPastDeadlineIsPending(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-value-read-late-live");
        var decisionClock = new FakeTimeProvider(Start);
        var readerClock = new FakeTimeProvider(Start + Ttl);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(decisionClock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), local, decisionClock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await using var readers = new ReplicaGroupCommitters([committer], committer.GroupId, new INodeLocatorCreateExpectations().Instance(), readerClock);
        var cache = new ReplicatedCache(local, readers);

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<object?>?>(cache.GetEntryAsync(CacheName, Key, cancellationToken));

        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ExpirationPendingDetail));
        _ = await Assert.That((await physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, Key), cancellationToken))?.Value).IsEqualTo("v");
    }

    /// <summary>A start the committer refuses fails every queued expired read with the expiration-pending refusal, and the entries stay in memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedStartFailsQueuedReadsRetryably(CancellationToken cancellationToken)
    {
        string[] keys = ["a", "b", "c"];
        using var dir = new TempDirectory("squirix-value-read-refused-start");
        var clock = new FakeTimeProvider(Start);
        var gateway = new ScriptedGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committer = CreateCommitter(registry, gateway, local, clock);
        foreach (var key in keys)
            await committer.CommitSetAsync(NewOperationId(), CacheName, key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);

        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        clock.Advance(Ttl);
        var cache = new ReplicatedCache(local, LeadOwn(committer));

        var reads = new Task<NodeCacheEntry<object?>?>[keys.Length];
        for (var i = 0; i < keys.Length; i++)
            reads[i] = cache.GetEntryAsync(CacheName, keys[i], cancellationToken).AsTask();

        foreach (var read in reads)
        {
            var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(read);
            _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ExpirationPendingDetail));
        }

        _ = await Assert.That((await physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, "a"), cancellationToken))?.Value).IsEqualTo("v");
    }

    private static async Task<string> LastKindAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var status = await log.GetStatusAsync(cancellationToken);
        var read = await log.ReadEntriesAsync(status.LastLogIndex, 1, cancellationToken);
        return (ReplicaLogCodec.Decode(read.Entries[0].Payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The log entry must decode."))).MutationKind;
    }
}
