using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
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
    private const string CacheName = JournalReplayKit.CacheName;
    private const string Key = JournalReplayKit.Key;

    private static readonly TimeSpan Downtime = TimeSpan.FromMinutes(8);
    private static readonly TimeSpan PastTtl = TimeSpan.FromMinutes(11);
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(10);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-expiry-replay";

    private JournalReplayKit Kit => new(Dir, _testMeter);

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
        var persistence = Kit.Persistence;
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

        var recovered = await Kit.RecoverAsync(new FakeTimeProvider(DateTimeOffset.UtcNow), false, cancellationToken);

        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>A put record whose deadline already passed is skipped on replay instead of failing recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastDeadlineReplaysAsExpired(CancellationToken cancellationToken)
    {
        var persistence = Kit.Persistence;
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

        var recovered = await Kit.RecoverAsync(new FakeTimeProvider(DateTimeOffset.UtcNow), false, cancellationToken);

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
        var recovered = await Kit.RecoverAsync(restartClock, false, cancellationToken);

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
        var recovered = await Kit.RecoverAsync(restartClock, false, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(deadline);
    }

    private Task<(DateTime WriteStart, NodeCacheEntry<string>? Memory)> WriteAsync(
        Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> mutate,
        TimeSpan readAfter,
        CancellationToken cancellationToken) => Kit.WriteAsync(mutate, readAfter, TimeSpan.Zero, 0, cancellationToken);
}
