using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
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
    private static readonly DateTimeOffset Restart = JournalReplayKit.WriteEpoch.AddHours(1);
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

    /// <summary>Under committed records an expired put record is replayed and compacted like a live one: only a committed record removes the key.</summary>
    /// <param name="compact">Whether the journal is compacted, on the restart clock, before the restart.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CommittedRecordsKeepExpiredPut(bool compact, CancellationToken cancellationToken)
    {
        var persistence = Kit.Persistence;
        var deadline = Restart.UtcDateTime.AddMinutes(-1);
        using (var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance))
        {
            await using var journal = JournalCoordinatorFactory.Create(
                persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                manifestStore,
                new AsyncManualResetEvent(true),
                NullLoggerFactory.Instance,
                TimeProvider.System,
                out _);
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(new NodeCacheEntry<object?>("earlier")), cancellationToken);
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(new NodeCacheEntry<object?>("later", expiresUtc: deadline)), cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var recovered = await Kit.RecoverAsync(new FakeTimeProvider(Restart), compact, CacheExpiryAuthority.CommittedRecords, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry?.Value).IsEqualTo("later");
        _ = await Assert.That(entry?.ExpiresUtc).IsEqualTo(deadline);
    }

    /// <summary>Under committed records a snapshot load keeps an entry whose deadline passed before the restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedRecordsKeepExpiredSnapshotEntry(CancellationToken cancellationToken)
    {
        var writeStart = await Kit.WriteSnapshotThenTailAsync(TimeSpan.Zero, Ttl, static (_, _) => ValueTask.CompletedTask, cancellationToken);

        var restart = new FakeTimeProvider(new DateTimeOffset(writeStart.Add(PastTtl), TimeSpan.Zero));
        var recovered = await Kit.RecoverAsync(restart, false, CacheExpiryAuthority.CommittedRecords, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry?.ExpiresUtc).IsEqualTo(writeStart.Add(Ttl));
    }

    /// <summary>An expired put record removes the earlier value of the key instead of leaving it live, with or without compaction.</summary>
    /// <param name="compact">Whether the journal is compacted, on the restart clock, before the restart.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ExpiredPutRemovesEarlierValue(bool compact, CancellationToken cancellationToken)
    {
        var persistence = Kit.Persistence;
        using (var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance))
        {
            await using var journal = JournalCoordinatorFactory.Create(
                persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                manifestStore,
                new AsyncManualResetEvent(true),
                NullLoggerFactory.Instance,
                TimeProvider.System,
                out _);
            var live = new NodeCacheEntry<object?>("earlier");
            var expired = new NodeCacheEntry<object?>("later", expiresUtc: Restart.UtcDateTime.AddMinutes(-1));
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(live), cancellationToken);
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(expired), cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var recovered = await Kit.RecoverAsync(new FakeTimeProvider(Restart), compact, cancellationToken);

        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>A put record whose deadline already passed is skipped on replay instead of failing recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastDeadlineReplaysAsExpired(CancellationToken cancellationToken)
    {
        var persistence = Kit.Persistence;
        using (var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance))
        {
            await using var journal = JournalCoordinatorFactory.Create(
                persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                manifestStore,
                new AsyncManualResetEvent(true),
                NullLoggerFactory.Instance,
                TimeProvider.System,
                out _);
            var expired = new NodeCacheEntry<object?>("v", expiresUtc: Restart.UtcDateTime.AddMinutes(-1));
            await journal.AppendPutUnderGateAsync(new CacheKey(CacheName, Key), JournalEntryPayloadKit.Encode(expired), cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var recovered = await Kit.RecoverAsync(new FakeTimeProvider(Restart), false, cancellationToken);

        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>Replay judges a journaled deadline on the server clock: live before it on that clock, skipped after it.</summary>
    /// <param name="compact">Whether the journal is compacted, on the same clock, before the restart.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReplayJudgesExpiryOnServerClock(bool compact, CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static (cache, ct) => cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: Ttl), ct),
            TimeSpan.Zero,
            cancellationToken);

        var live = await Kit.RecoverAsync(new FakeTimeProvider(new DateTimeOffset(written.WriteStart.AddMinutes(1), TimeSpan.Zero)), compact, cancellationToken);
        var liveEntry = await live.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        var expired = await Kit.RecoverAsync(new FakeTimeProvider(new DateTimeOffset(written.WriteStart.Add(PastTtl), TimeSpan.Zero)), compact, cancellationToken);

        _ = await Assert.That(liveEntry?.ExpiresUtc).IsEqualTo(written.WriteStart.Add(Ttl));
        _ = await Assert.That(await expired.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>A snapshot load judges a captured deadline on the server clock of the restart, not on the wall clock of the host.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotLoadJudgesExpiryOnServerClock(CancellationToken cancellationToken)
    {
        var writeStart = await Kit.WriteSnapshotThenTailAsync(TimeSpan.Zero, Ttl, static (_, _) => ValueTask.CompletedTask, cancellationToken);

        var live = await Kit.RecoverAsync(new FakeTimeProvider(new DateTimeOffset(writeStart.AddMinutes(1), TimeSpan.Zero)), false, cancellationToken);
        var liveEntry = await live.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        var expired = await Kit.RecoverAsync(new FakeTimeProvider(new DateTimeOffset(writeStart.Add(PastTtl), TimeSpan.Zero)), false, cancellationToken);

        _ = await Assert.That(liveEntry?.ExpiresUtc).IsEqualTo(writeStart.Add(Ttl));
        _ = await Assert.That(await expired.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
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
