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
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Threading;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Writes cache mutations through a local-owner journal decorator on a controllable clock, then recovers the journal, so replay tests
/// share one harness: the persistence options, the millisecond-aligned write clock and the recovery wiring.
/// </summary>
/// <remarks>Recovery reads the wall clock, so write clocks start in the past and deadlines are measured in hours.</remarks>
[Immutable]
internal sealed class JournalReplayKit
{
    internal const string CacheName = "cache";
    internal const string Key = "k";
    internal const string Self = "node-a";

    private readonly Meter _meter;

    internal JournalReplayKit(string dir, Meter meter, bool groupCommit = false)
    {
        _meter = meter;
        Dir = dir;
        Persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 5,
            ManifestRetentionCount = 1,
            JournalGroupCommitMaxWait = groupCommit ? TimeSpan.FromMilliseconds(1) : TimeSpan.Zero,
        };
    }

    /// <summary>Gets the data directory.</summary>
    internal string Dir { get; }

    /// <summary>Gets the persistence options every write and recovery uses.</summary>
    internal PersistenceOptions Persistence { get; }

    /// <summary>Creates a write clock relative to real time, aligned to the whole millisecond the journal stores deadlines at.</summary>
    /// <param name="startOffset">The offset from real time; negative puts the start in the past.</param>
    /// <param name="extraTicks">Ticks added after the alignment, to start off the millisecond boundary.</param>
    /// <returns>The clock.</returns>
    internal static FakeTimeProvider CreateWriteClock(TimeSpan startOffset, long extraTicks = 0)
    {
        var now = DateTimeOffset.UtcNow.Add(startOffset);
        return new FakeTimeProvider(now.AddTicks(-(now.Ticks % TimeSpan.TicksPerMillisecond) + extraTicks));
    }

    /// <summary>Counts the records in the journal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The record count.</returns>
    internal int CountJournalRecords(CancellationToken cancellationToken)
    {
        var count = 0;
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
            count++;

        return count;
    }

    /// <summary>Opens a local-owner decorator over a fresh physical cache and a journal.</summary>
    /// <param name="clock">The clock the decorator and the cache share.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The open session.</returns>
    internal async Task<Session> OpenAsync(FakeTimeProvider clock, CancellationToken cancellationToken)
    {
        var manifestStore = new Ledger(Persistence, NullLogger<Ledger>.Instance);
        var journal = JournalCoordinatorFactory.Create(
            Persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            NullLogger.Instance);
        var physical = new PhysicalCache<string>(clock);
        var cache = new JournalLoggingCacheDecorator<string>(
            Self,
            RocksDoubles.CreateOwnerLocator(Self),
            new ClientCache<string>(physical, physical),
            journal,
            new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance),
            clock,
            physical.RawReader);
        return new Session(manifestStore, journal, physical, cache, clock);
    }

    /// <summary>Reads the last put record of the journal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The decoded entry of the last put record.</returns>
    /// <exception cref="InvalidOperationException">The journal holds no decodable put record.</exception>
    internal NodeCacheEntry<string> ReadLastJournaledPut(CancellationToken cancellationToken)
    {
        NodeCacheEntry<string>? last = null;
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
        {
            if (records.Current.Operation == JournalOperationKind.Put && JournalEntryPayload.TryDecode<string>(records.Current.PutEntryBytes.Span, out var entry))
                last = entry;
        }

        return ThrowHelper.Required(last, "The journal holds no decodable put record.");
    }

    /// <summary>Recovers the journal into a fresh cache, optionally compacting it first.</summary>
    /// <param name="clock">The clock of the recovered cache.</param>
    /// <param name="compact">Whether the journal is compacted before recovery.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The recovered cache.</returns>
    internal async Task<PhysicalCache<string>> RecoverAsync(TimeProvider clock, bool compact, CancellationToken cancellationToken)
    {
        using var manifestStore = new Ledger(Persistence, NullLogger<Ledger>.Instance);
        if (compact)
            await JournalCompactor.CompactAsync(Persistence, manifestStore, StoreFactory.CreateReader(), cancellationToken);

        var cache = new PhysicalCache<string>(clock);
        var dependencies = new RecoveryDependencies<string>(
            Persistence,
            manifestStore,
            cache,
            new AsyncManualResetEvent(true),
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_meter)),
            StoreFactory.CreateReader());
        await new RecoveryService<string>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<string>>.Instance, dependencies).StartAsync(cancellationToken);
        return cache;
    }

    /// <summary>Runs the mutation, which does not need the clock, against a local-owner decorator whose clock starts at <paramref name="startOffset" /> from real time.</summary>
    /// <param name="mutate">The mutation to journal.</param>
    /// <param name="readAfter">How far the clock advances before memory is read.</param>
    /// <param name="startOffset">The offset of the write clock start from real time.</param>
    /// <param name="extraTicks">Ticks added to the millisecond-aligned start.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The write clock start and the entry memory holds afterwards.</returns>
    internal Task<(DateTime WriteStart, NodeCacheEntry<string>? Memory)> WriteAsync(
        Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> mutate,
        TimeSpan readAfter,
        TimeSpan startOffset,
        long extraTicks,
        CancellationToken cancellationToken) => WriteAsync((cache, _, ct) => mutate(cache, ct), readAfter, startOffset, extraTicks, cancellationToken);

    /// <summary>Runs the mutation against a local-owner decorator whose clock starts at <paramref name="startOffset" /> from real time.</summary>
    /// <param name="mutate">The mutation to journal.</param>
    /// <param name="readAfter">How far the clock advances before memory is read.</param>
    /// <param name="startOffset">The offset of the write clock start from real time.</param>
    /// <param name="extraTicks">Ticks added to the millisecond-aligned start.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The write clock start and the entry memory holds afterwards.</returns>
    internal async Task<(DateTime WriteStart, NodeCacheEntry<string>? Memory)> WriteAsync(
        Func<JournalLoggingCacheDecorator<string>, FakeTimeProvider, CancellationToken, ValueTask> mutate,
        TimeSpan readAfter,
        TimeSpan startOffset,
        long extraTicks,
        CancellationToken cancellationToken)
    {
        var clock = CreateWriteClock(startOffset, extraTicks);
        await using var session = await OpenAsync(clock, cancellationToken);
        var writeStart = clock.GetUtcNow().UtcDateTime;
        await mutate(session.Cache, clock, cancellationToken);
        clock.Advance(readAfter);
        return (writeStart, await session.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken));
    }

    /// <summary>Writes an entry with a one-hour deadline, cuts a snapshot that holds it, then runs the tail mutation after the cut.</summary>
    /// <param name="startOffset">The offset of the write clock start from real time.</param>
    /// <param name="ttl">The relative expiration of the entry the snapshot holds.</param>
    /// <param name="tail">The mutation journaled after the snapshot cut.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The write clock start.</returns>
    internal async Task<DateTime> WriteSnapshotThenTailAsync(
        TimeSpan startOffset,
        TimeSpan ttl,
        Func<JournalLoggingCacheDecorator<string>, CancellationToken, ValueTask> tail,
        CancellationToken cancellationToken)
    {
        var clock = CreateWriteClock(startOffset);
        await using var session = await OpenAsync(clock, cancellationToken);
        var coordinator = (await Assert.That(session.Journal).IsTypeOf<JournalCoordinator>())!;
        var writeStart = clock.GetUtcNow().UtcDateTime;
        await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v", expiration: ttl), cancellationToken);
        var snapshotted = await session.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(snapshotted).IsNotNull();

        var cut = (session.ManifestStore, writer: StoreFactory.CreateWriter(Persistence), entry: snapshotted!, coordinator);
        _ = await coordinator.ExecuteSnapshotCutAsync(
            cut,
            static (state, _, _) => new ValueTask<(int ReplayFromSegment, ulong NextSequence)>((state.coordinator.CurrentSegmentIndex, state.coordinator.NextSequence)),
            static async (state, seqAtFlush, boundary, ct) =>
            {
                var previous = await state.ManifestStore.ReadCurrentOrDefaultAsync(ct).ConfigureAwait(false);
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
                await state.ManifestStore.WriteAsync(updated, ct).ConfigureAwait(false);
                return updated.LastSnapshot;
            },
            cancellationToken);

        await tail(session.Cache, cancellationToken);
        return writeStart;
    }

    /// <summary>An open journal, physical cache and local-owner decorator; disposing it closes the journal.</summary>
    [Immutable]
    internal sealed class Session : IAsyncDisposable
    {
        internal Session(Ledger manifestStore, IJournalCoordinator journal, PhysicalCache<string> physical, JournalLoggingCacheDecorator<string> cache, FakeTimeProvider clock)
        {
            ManifestStore = manifestStore;
            Journal = journal;
            Physical = physical;
            Cache = cache;
            Clock = clock;
        }

        /// <summary>Gets the decorator under test.</summary>
        internal JournalLoggingCacheDecorator<string> Cache { get; }

        /// <summary>Gets the write clock.</summary>
        internal FakeTimeProvider Clock { get; }

        /// <summary>Gets the journal coordinator.</summary>
        internal IJournalCoordinator Journal { get; }

        /// <summary>Gets the manifest store.</summary>
        internal Ledger ManifestStore { get; }

        /// <summary>Gets the memory the decorator applies to.</summary>
        internal PhysicalCache<string> Physical { get; }

        /// <inheritdoc />
        public async ValueTask DisposeAsync()
        {
            await Journal.DisposeAsync();
            ManifestStore.Dispose();
        }
    }
}
