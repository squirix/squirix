using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
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
    /// A hit on a live entry with a deadline reads the value without building the entry, so it allocates no more than the local value
    /// read under it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LiveHitAllocatesAsLocalValueRead(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-value-read-alloc");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), local, clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, Key, new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var cache = new ReplicatedCache(local, LeadOwn(committer));

        var replicated = await AllocatedByReadsAsync(cache, cancellationToken);
        var direct = await AllocatedByReadsAsync(local, cancellationToken);

        _ = await Assert.That(replicated).IsEqualTo(direct);
    }

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
        _ = await Assert.That(miss).IsEqualTo(default(NodeCacheValueRead<object?>));
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

    private static async Task<long> AllocatedByReadsAsync(ILogicalNamespacedCache<object?> cache, CancellationToken cancellationToken)
    {
        const int calls = 1000;
        for (var i = 0; i < calls; i++)
            _ = await cache.GetValueAsync(CacheName, Key, cancellationToken);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < calls; i++)
            _ = await cache.GetValueAsync(CacheName, Key, cancellationToken);

        return GC.GetAllocatedBytesForCurrentThread() - before;
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
