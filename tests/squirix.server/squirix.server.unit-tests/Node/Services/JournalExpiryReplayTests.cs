using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Journal replay must restore the expiry deadline fixed at write time: a relative TTL journaled by a local-owner
/// mutation may not be re-anchored to the restart clock, which would stretch it by the downtime.
/// </summary>
[Immutable]
public sealed class JournalExpiryReplayTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const string Self = "node-a";

    private static readonly TimeSpan Downtime = TimeSpan.FromMinutes(8);
    private static readonly TimeSpan PastTtl = TimeSpan.FromMinutes(11);
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(10);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-expiry-replay";

    /// <summary>A relative-TTL add replays with its write-time deadline, not one re-anchored to the restart clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddReplaysWriteTimeDeadline(CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) => _ = await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: Ttl), ct),
            TimeSpan.Zero,
            cancellationToken);

        await AssertReplayedDeadlineAsync(written, cancellationToken);
    }

    /// <summary>An expired put record removes the earlier value of the key instead of leaving it live.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredPutRemovesEarlierValue(CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence();
        using (var manifestStore = new Ledger(persistence))
        {
            await using var journal = JournalCoordinatorFactory.Create(
                persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                manifestStore,
                new AsyncManualResetEvent(true));
            var live = new NodeCacheEntry<object?>("earlier");
            var expired = new NodeCacheEntry<object?>("later", expiresUtc: DateTime.UtcNow.AddMinutes(-1));
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(live), cancellationToken);
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(expired), cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var recovered = await RecoverAsync(persistence, new FakeTimeProvider(DateTimeOffset.UtcNow), cancellationToken);

        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>A put record whose deadline already passed is skipped on replay instead of failing recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastDeadlineReplaysAsExpired(CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence();
        using (var manifestStore = new Ledger(persistence))
        {
            await using var journal = JournalCoordinatorFactory.Create(
                persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                manifestStore,
                new AsyncManualResetEvent(true));
            var expired = new NodeCacheEntry<object?>("v", expiresUtc: DateTime.UtcNow.AddMinutes(-1));
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(expired), cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var recovered = await RecoverAsync(persistence, new FakeTimeProvider(DateTimeOffset.UtcNow), cancellationToken);

        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>A relative-TTL set replays with its write-time deadline, not one re-anchored to the restart clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReplaysWriteTimeDeadline(CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static (cache, ct) => cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: Ttl), ct),
            TimeSpan.Zero,
            cancellationToken);

        await AssertReplayedDeadlineAsync(written, cancellationToken);
    }

    /// <summary>A set without expiry clears a previous TTL, in memory and on replay alike.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetWithoutExpiryClearsTtl(CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v1", expiration: Ttl), ct);
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v2"), ct);
            },
            PastTtl,
            cancellationToken);

        // Memory is read after the clock passed the old TTL: the entry must still be there, without expiry.
        _ = await Assert.That(written.Memory).IsNotNull();
        _ = await Assert.That(written.Memory!.Value).IsEqualTo("v2");
        _ = await Assert.That(written.Memory.ExpiresUtc).IsNull();

        var restartClock = new FakeTimeProvider(new DateTimeOffset(written.WriteStart.Add(PastTtl), TimeSpan.Zero));
        var recovered = await RecoverAsync(CreatePersistence(), restartClock, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("v2");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
    }

    /// <summary>A touch replays with its write-time deadline, not one re-anchored to the restart clock.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchReplaysWriteTimeDeadline(CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) =>
            {
                _ = await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v"), ct);
                _ = await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, Ttl, ct);
            },
            TimeSpan.Zero,
            cancellationToken);

        await AssertReplayedDeadlineAsync(written, cancellationToken);
    }

    /// <summary>An update of a relative-TTL entry replays with the original write-time deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateReplaysWriteTimeDeadline(CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v1", expiration: Ttl), ct);
                _ = await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "v2", ct);
            },
            TimeSpan.Zero,
            cancellationToken);

        await AssertReplayedDeadlineAsync(written, cancellationToken);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private async Task AssertReplayedDeadlineAsync((DateTime WriteStart, NodeCacheEntry<string>? Memory) written, CancellationToken cancellationToken)
    {
        var deadline = written.WriteStart.Add(Ttl);
        _ = await Assert.That(written.Memory?.ExpiresUtc).IsEqualTo(deadline);

        // Restart after a downtime that is shorter than the TTL: the entry must still be live and keep the deadline memory held.
        var restartClock = new FakeTimeProvider(new DateTimeOffset(written.WriteStart.Add(Downtime), TimeSpan.Zero));
        var recovered = await RecoverAsync(CreatePersistence(), restartClock, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(deadline);
    }

    private PersistenceOptions CreatePersistence() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 1,
        FlushInterval = 5,
        ManifestRetentionCount = 1,
    };

    private async Task<PhysicalCache<string>> RecoverAsync(PersistenceOptions persistence, TimeProvider clock, CancellationToken cancellationToken)
    {
        using var manifestStore = new Ledger(persistence);
        var cache = new PhysicalCache<string>(clock);
        var dependencies = new RecoveryDependencies<string>(
            persistence,
            manifestStore,
            cache,
            new AsyncManualResetEvent(true),
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter)),
            StoreFactory.CreateReader());
        await new RecoveryService<string>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<string>>.Instance, dependencies).StartAsync(cancellationToken);
        return cache;
    }

    private async Task<(DateTime WriteStart, NodeCacheEntry<string>? Memory)> WriteAsync(
        Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> mutate,
        TimeSpan readAfter,
        CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence();
        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));

        // Start the write clock at real time so replay does not skip the entry as expired, aligned to the whole
        // millisecond the journal stores deadlines at, so the replayed deadline compares exactly.
        var now = DateTimeOffset.UtcNow;
        var writeClock = new FakeTimeProvider(now.AddTicks(-(now.Ticks % TimeSpan.TicksPerMillisecond)));
        var physical = new PhysicalCache<string>(writeClock);
        var cache = new JournalLoggingCacheDecorator<string>(
            Self,
            RocksDoubles.CreateOwnerLocator(Self),
            new ClientCache<string>(physical, physical),
            journal,
            new DurableMutationExecutor(journal),
            writeClock);

        var writeStart = writeClock.GetUtcNow().UtcDateTime;
        await mutate(cache, cancellationToken);
        writeClock.Advance(readAfter);
        return (writeStart, await physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken));
    }
}
