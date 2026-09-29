using System;
using System.Collections.Frozen;
using System.Collections.Generic;
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
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A local-owner touch, expiration removal or update journals the entry it decided, so replay, compaction and snapshot plus tail
/// recovery restore the state of the last frame of a key even after the deadline of the entry the frame replaced has passed.
/// </summary>
/// <remarks>Recovery reads the wall clock, so the write clock starts two hours in the past and the deadlines are measured in hours.</remarks>
[Immutable]
public sealed class JournalDecidedEntryReplayTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const string Self = "node-a";

    private static readonly TimeSpan ExtendedTtl = TimeSpan.FromHours(3);
    private static readonly TimeSpan OriginalTtl = TimeSpan.FromHours(1);
    private static readonly TimeSpan PastStart = TimeSpan.FromHours(-2);

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-decided-replay";

    /// <summary>Removing the expiration before the deadline keeps the key without a deadline across a restart after the original deadline.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PersistBeforeDeadlineSurvivesRestart(bool compact, CancellationToken cancellationToken)
    {
        _ = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: OriginalTtl), ct);
                _ = await Assert.That(await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await RecoverAsync(compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("v");
        _ = await Assert.That(entry.ExpiresUtc).IsNull();
    }

    /// <summary>Removing the expiration after a snapshot cut keeps the key without a deadline although the snapshot entry has since expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotThenPersistInTailSurvivesRestart(CancellationToken cancellationToken)
    {
        _ = await WriteSnapshotThenTailAsync(
            static async (cache, ct) => _ = await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, ct),
            cancellationToken);

        var recovered = await RecoverAsync(false, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsNull();
    }

    /// <summary>Extending the expiration after a snapshot cut keeps the key until the extended deadline although the snapshot entry has since expired.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotThenTouchInTailSurvivesRestart(CancellationToken cancellationToken)
    {
        var start = await WriteSnapshotThenTailAsync(
            static async (cache, ct) => _ = await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct),
            cancellationToken);

        var recovered = await RecoverAsync(false, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(start.Add(ExtendedTtl));
    }

    /// <summary>A touch of an entry whose deadline passed changes nothing and writes no frame, so the key stays absent after a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAfterDeadlineIsNotJournaled(CancellationToken cancellationToken)
    {
        var memory = await WriteWithClockAsync(
            static async (cache, clock, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: OriginalTtl), ct);
                clock.Advance(OriginalTtl + TimeSpan.FromMinutes(1));
                _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct)).IsFalse();
            },
            cancellationToken);

        _ = await Assert.That(memory.Memory).IsNull();
        _ = await Assert.That(CountJournalRecords(cancellationToken)).IsEqualTo(1);
        var recovered = await RecoverAsync(false, cancellationToken);
        _ = await Assert.That(await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
    }

    /// <summary>Extending the expiration before the deadline keeps the key until exactly the decided deadline across a restart after the original deadline.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TouchBeforeDeadlineSurvivesRestart(bool compact, CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: OriginalTtl), ct);
                _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await RecoverAsync(compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(written.Add(ExtendedTtl));
    }

    /// <summary>An update after a touch journals the touched deadline, not the original one.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UpdateAfterTouchKeepsTouchedDeadline(bool compact, CancellationToken cancellationToken)
    {
        var written = await WriteAsync(
            static async (cache, ct) =>
            {
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v1", expiration: OriginalTtl), ct);
                _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, ExtendedTtl, ct)).IsTrue();
                _ = await Assert.That(await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "v2", ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await RecoverAsync(compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("v2");
        _ = await Assert.That(entry.ExpiresUtc).IsEqualTo(written.Add(ExtendedTtl));
    }

    /// <summary>An update keeps the tags and the version of the entry across a restart.</summary>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UpdateKeepsTagsAcrossRestart(bool compact, CancellationToken cancellationToken)
    {
        _ = await WriteAsync(
            static async (cache, ct) =>
            {
                var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
                await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v1", 2, tags: tags), ct);
                _ = await Assert.That(await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "v2", ct)).IsTrue();
            },
            cancellationToken);

        var recovered = await RecoverAsync(compact, cancellationToken);

        var entry = await recovered.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.Value).IsEqualTo("v2");
        _ = await Assert.That(entry.Version).IsEqualTo(2);
        _ = await Assert.That(entry.Tags).IsNotNull();
        _ = await Assert.That(entry.Tags!["team"]).IsEqualTo("a");
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private static JournalLoggingCacheDecorator<string> CreateDecorator(IJournalCoordinator journal, PhysicalCache<string> physical, TimeProvider clock) => new(
        Self,
        RocksDoubles.CreateOwnerLocator(Self),
        new ClientCache<string>(physical, physical),
        journal,
        new DurableMutationExecutor(journal),
        clock);

    private static FakeTimeProvider CreateWriteClock()
    {
        // Aligned to the whole millisecond the journal stores deadlines at, so a replayed deadline compares exactly.
        var now = DateTimeOffset.UtcNow.Add(PastStart);
        return new FakeTimeProvider(now.AddTicks(-(now.Ticks % TimeSpan.TicksPerMillisecond)));
    }

    private int CountJournalRecords(CancellationToken cancellationToken)
    {
        var count = 0;
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
            count++;

        return count;
    }

    private PersistenceOptions CreatePersistence() => new()
    {
        DataDir = Dir,
        JournalMaxSegmentMb = 1,
        FlushInterval = 5,
        ManifestRetentionCount = 1,
    };

    private async Task<PhysicalCache<string>> RecoverAsync(bool compact, CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence();
        using var manifestStore = new Ledger(persistence);
        if (compact)
            await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(), cancellationToken);

        var cache = new PhysicalCache<string>();
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

    private async Task<DateTime> WriteAsync(Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> mutate, CancellationToken cancellationToken)
    {
        var written = await WriteWithClockAsync((cache, _, ct) => mutate(cache, ct), cancellationToken);
        return written.WriteStart;
    }

    /// <summary>Runs the mutation against a local-owner decorator whose clock starts two hours in the past.</summary>
    /// <param name="mutate">The mutation to journal.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The write clock start and the entry memory holds afterwards.</returns>
    private async Task<(DateTime WriteStart, NodeCacheEntry<string>? Memory)> WriteWithClockAsync(
        Func<JournalLoggingCacheDecorator<string>, FakeTimeProvider, CancellationToken, ValueTask> mutate,
        CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence();
        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var writeClock = CreateWriteClock();
        var physical = new PhysicalCache<string>(writeClock);
        var cache = CreateDecorator(journal, physical, writeClock);

        var writeStart = writeClock.GetUtcNow().UtcDateTime;
        await mutate(cache, writeClock, cancellationToken);
        return (writeStart, await physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken));
    }

    /// <summary>Writes an entry with the original deadline, cuts a snapshot that holds it, then runs the tail mutation after the cut.</summary>
    /// <param name="tail">The mutation journaled after the snapshot cut.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The write clock start; the original deadline has passed on the recovery clock and any extended deadline has not.</returns>
    private async Task<DateTime> WriteSnapshotThenTailAsync(Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> tail, CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence();
        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var coordinator = (await Assert.That(journal).IsTypeOf<JournalCoordinator>())!;
        var writeClock = CreateWriteClock();
        var physical = new PhysicalCache<string>(writeClock);
        var cache = CreateDecorator(journal, physical, writeClock);

        var writeStart = writeClock.GetUtcNow().UtcDateTime;
        await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: OriginalTtl), cancellationToken);
        var snapshotted = await physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(snapshotted).IsNotNull();

        var cut = (manifestStore, writer: StoreFactory.CreateWriter(persistence), entry: snapshotted!, coordinator);
        _ = await coordinator.ExecuteSnapshotCutAsync(
            cut,
            static (state, _, _) => new ValueTask<(int ReplayFromSegment, ulong NextSequence)>((state.coordinator.CurrentSegmentIndex, state.coordinator.NextSequence)),
            static async (state, seqAtFlush, boundary, ct) =>
            {
                var previous = await state.manifestStore.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false);
                var nextIndex = (previous.LastSnapshot?.Index ?? 0) + 1;
                var entry = new NodeCacheEntry<object?> { Value = state.entry.Value, Version = state.entry.Version, ExpiresUtc = state.entry.ExpiresUtc };
                var path = await state.writer.WriteSingleAsync(nextIndex, new CacheKey(CacheName, Key), entry, ct).ConfigureAwait(false);
                var updated = new State
                {
                    Format = previous.Format,
                    CurrentJournal = previous.CurrentJournal,
                    NextSequence = boundary.NextSequence,
                    LastSnapshot = new SnapshotRef
                    {
                        Index = nextIndex,
                        Path = path,
                        CreatedUtc = DateTime.UtcNow,
                        LastAppliedSequence = seqAtFlush,
                        ReplayFromJournalSegment = boundary.ReplayFromSegment,
                    },
                };
                await state.manifestStore.WriteAsync(updated, ct).ConfigureAwait(false);
                return updated.LastSnapshot;
            },
            cancellationToken);

        await tail(cache, cancellationToken);
        return writeStart;
    }
}
