using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>
/// A local-owner mutation must apply to memory the exact expiry deadline it journals, so a restart restores the
/// deadline memory held. The inner cache advances the shared clock before it applies, standing in for the key-lock
/// wait and durable commit that separate the journal write from the memory apply.
/// </summary>
[Immutable]
public sealed class JournalExpiryApplyTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const string Self = "node-a";

    private static readonly TimeSpan ApplyDelay = TimeSpan.FromMinutes(5);
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Ttl = TimeSpan.FromMinutes(10);

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-expiry-apply";

    /// <summary>A relative-TTL add applies the journaled deadline, not one recomputed when memory applies it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AddAppliesJournaledDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var cache = harness.CreateDecorator(CreateDelayingInner(harness.Real, clock));
            _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, CreateRelativeEntry(), cancellationToken)).IsTrue();
            await AssertMemoryDeadlineAsync(harness.Physical, cancellationToken);
        }

        await AssertJournaledPutDeadlineAsync(cancellationToken);
    }

    /// <summary>A relative-TTL set whose deadline passes before memory applies it lands expired instead of failing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastDeadlineAppliesAsExpired(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var cache = harness.CreateDecorator(CreateDelayingInner(harness.Real, clock));
            var entry = new NodeCacheEntry<string>("v", expiration: ApplyDelay / 2);

            await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, entry, cancellationToken);

            _ = await Assert.That(await harness.Physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken)).IsNull();
        }

        var journaled = ReadJournaledPut(cancellationToken);
        _ = await Assert.That(journaled.ExpiresUtc).IsEqualTo(Start.UtcDateTime.Add(ApplyDelay / 2));
    }

    /// <summary>The payload-prepare entry point hands memory the same resolved add it journals.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreparedAddAppliesJournaledDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var prepare = new JournalPayloadPrepareCacheDecorator<string>(
                Self,
                RocksDoubles.CreateOwnerLocator(Self),
                harness.CreateDecorator(CreateDelayingInner(harness.Real, clock)));
            _ = await Assert.That(await prepare.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, CreateRelativeEntry(), cancellationToken)).IsTrue();
            await AssertMemoryDeadlineAsync(harness.Physical, cancellationToken);
        }

        await AssertJournaledPutDeadlineAsync(cancellationToken);
    }

    /// <summary>The payload-prepare entry point hands memory the same resolved set it journals.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreparedSetAppliesJournaledDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var prepare = new JournalPayloadPrepareCacheDecorator<string>(
                Self,
                RocksDoubles.CreateOwnerLocator(Self),
                harness.CreateDecorator(CreateDelayingInner(harness.Real, clock)));
            await prepare.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, CreateRelativeEntry(), cancellationToken);
            await AssertMemoryDeadlineAsync(harness.Physical, cancellationToken);
        }

        await AssertJournaledPutDeadlineAsync(cancellationToken);
    }

    /// <summary>A relative-TTL set applies the journaled deadline, not one recomputed when memory applies it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetAppliesJournaledDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var cache = harness.CreateDecorator(CreateDelayingInner(harness.Real, clock));
            await cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, Key, CreateRelativeEntry(), cancellationToken);
            await AssertMemoryDeadlineAsync(harness.Physical, cancellationToken);
        }

        await AssertJournaledPutDeadlineAsync(cancellationToken);
    }

    /// <summary>A remove of the expiration journals a put of the entry without a deadline and applies exactly that entry to memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PersistJournalsAppliedEntry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            await SeedTaggedEntryAsync(harness, cancellationToken);
            var cache = harness.CreateDecorator(CreateDelayingInner(harness.Real, clock));
            _ = await Assert.That(await cache.RemoveExpirationAsync(UnitMutationOpIds.Default, CacheName, Key, cancellationToken)).IsTrue();
            await AssertMemoryTaggedAsync(harness.Physical, cancellationToken);
        }

        var journaled = ReadJournaledPut(cancellationToken);
        _ = await Assert.That(journaled.ExpiresUtc).IsNull();
        _ = await Assert.That(journaled.Version).IsEqualTo(3);
        _ = await Assert.That(journaled.Tags).IsNotNull();
        _ = await Assert.That(journaled.Tags!["team"]).IsEqualTo("a");
    }

    /// <summary>A touch applies the journaled deadline even when the clock advances before memory applies it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAppliesJournaledDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var seeding = harness.CreateDecorator(harness.Real);
            _ = await Assert.That(await seeding.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v"), cancellationToken)).IsTrue();
            var cache = harness.CreateDecorator(CreateDelayingInner(harness.Real, clock));
            _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, Ttl, cancellationToken)).IsTrue();
            await AssertMemoryDeadlineAsync(harness.Physical, cancellationToken);
        }

        await AssertJournaledPutDeadlineAsync(cancellationToken);
    }

    /// <summary>A touch decided from a raw read applies the journaled deadline even when the clock advances before memory applies it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RawReadTouchAppliesJournaledDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var seeding = harness.CreateDecorator(harness.Real);
            _ = await Assert.That(await seeding.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v"), cancellationToken)).IsTrue();
            var cache = harness.CreateDecorator(CreateDelayingInner(harness.Real, clock), true);
            _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, Ttl, cancellationToken)).IsTrue();
            await AssertMemoryDeadlineAsync(harness.Physical, cancellationToken);
        }

        await AssertJournaledPutDeadlineAsync(cancellationToken);
    }

    /// <summary>A touch journals its deadline from the injected server clock as a put of the whole entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchJournalsServerClockDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            var cache = harness.CreateDecorator(harness.Real);
            _ = await Assert.That(await cache.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, new NodeCacheEntry<string>("v"), cancellationToken)).IsTrue();
            _ = await Assert.That(await cache.TouchAsync(UnitMutationOpIds.Default, CacheName, Key, Ttl, cancellationToken)).IsTrue();
            await AssertMemoryDeadlineAsync(harness.Physical, cancellationToken);
        }

        await AssertJournaledPutDeadlineAsync(cancellationToken);
    }

    /// <summary>An update journals the replacement with the tags, version and deadline of the entry and applies exactly that entry to memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UpdateJournalsAppliedEntry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        await using (var harness = await Harness.CreateAsync(Dir, clock, cancellationToken))
        {
            await SeedTaggedEntryAsync(harness, cancellationToken);
            var cache = harness.CreateDecorator(CreateDelayingInner(harness.Real, clock));
            _ = await Assert.That(await cache.UpdateAsync(UnitMutationOpIds.Default, CacheName, Key, "v2", cancellationToken)).IsTrue();
            await AssertMemoryTaggedAsync(harness.Physical, cancellationToken);
        }

        var journaled = ReadJournaledPut(cancellationToken);
        _ = await Assert.That(journaled.Value).IsEqualTo("v2");
        _ = await Assert.That(journaled.Version).IsEqualTo(3);
        _ = await Assert.That(journaled.ExpiresUtc).IsEqualTo(Start.UtcDateTime.Add(Ttl));
        _ = await Assert.That(journaled.Tags).IsNotNull();
        _ = await Assert.That(journaled.Tags!["team"]).IsEqualTo("a");
    }

    private static async Task AssertMemoryTaggedAsync(PhysicalCache<string> physical, CancellationToken cancellationToken)
    {
        var memory = await physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(memory).IsNotNull();
        _ = await Assert.That(memory!.Version).IsEqualTo(3);
        _ = await Assert.That(memory.Tags).IsNotNull();
        _ = await Assert.That(memory.Tags!["team"]).IsEqualTo("a");
    }

    private static async Task SeedTaggedEntryAsync(Harness harness, CancellationToken cancellationToken)
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "a" }.ToFrozenDictionary(StringComparer.Ordinal);
        var seeded = new NodeCacheEntry<string>("v1", 3, Start.UtcDateTime.Add(Ttl), tags: tags);
        var seeding = harness.CreateDecorator(harness.Real);
        _ = await Assert.That(await seeding.TryAddEntryAsync(UnitMutationOpIds.Default, CacheName, Key, seeded, cancellationToken)).IsTrue();
    }

    private static async Task AssertMemoryDeadlineAsync(PhysicalCache<string> physical, CancellationToken cancellationToken)
    {
        var entry = await physical.GetEntryAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(entry).IsNotNull();
        _ = await Assert.That(entry!.ExpiresUtc).IsEqualTo(Start.UtcDateTime.Add(Ttl));
    }

    private static ILogicalNamespacedCache<string> CreateDelayingInner(ClientCache<string> real, FakeTimeProvider clock)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = inner.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Callback(real.GetValueAsync);
        _ = inner.Setups.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>()).Callback(real.GetEntryAsync);
        _ = inner.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                 .Callback((operationId, cacheName, key, entry, token) =>
                  {
                      clock.Advance(ApplyDelay);
                      return real.SetEntryAsync(operationId, cacheName, key, entry, token);
                  });
        _ = inner.Setups.TryAddEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                 .Callback((operationId, cacheName, key, entry, token) =>
                  {
                      clock.Advance(ApplyDelay);
                      return real.TryAddEntryAsync(operationId, cacheName, key, entry, token);
                  });
        return inner.Instance();
    }

    private static NodeCacheEntry<string> CreateRelativeEntry() => new("v", expiration: Ttl);

    private async Task AssertJournaledPutDeadlineAsync(CancellationToken cancellationToken)
    {
        var journaled = ReadJournaledPut(cancellationToken);
        _ = await Assert.That(journaled.Expiration).IsNull();
        _ = await Assert.That(journaled.ExpiresUtc).IsEqualTo(Start.UtcDateTime.Add(Ttl));
    }

    private NodeCacheEntry<string> ReadJournaledPut(CancellationToken cancellationToken)
    {
        NodeCacheEntry<string>? last = null;
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
        {
            if (records.Current.Operation == JournalOperationKind.Put && JournalEntryPayload.TryDecode<string>(records.Current.PutEntryBytes.Span, out var entry))
                last = entry;
        }

        return last ?? ThrowHelper.Throw<NodeCacheEntry<string>>(new InvalidOperationException("The journal holds no decodable put record."));
    }

    [Immutable]
    private sealed class Harness : IAsyncDisposable
    {
        private readonly TimeProvider _clock;
        private readonly Ledger _manifestStore;

        private Harness(Ledger manifestStore, IJournalCoordinator journal, TimeProvider clock)
        {
            _manifestStore = manifestStore;
            Journal = journal;
            _clock = clock;
            Physical = new PhysicalCache<string>(clock);
            Real = new ClientCache<string>(Physical, Physical);
        }

        internal IJournalCoordinator Journal { get; }

        internal PhysicalCache<string> Physical { get; }

        internal ClientCache<string> Real { get; }

        public async ValueTask DisposeAsync()
        {
            await Journal.DisposeAsync();
            _manifestStore.Dispose();
        }

        internal static async Task<Harness> CreateAsync(string dataDir, TimeProvider clock, CancellationToken cancellationToken)
        {
            var options = new PersistenceOptions
            {
                DataDir = dataDir,
                JournalMaxSegmentMb = 1,
                FlushInterval = 5,
                ManifestRetentionCount = 1,
            };
            var manifestStore = new Ledger(options, NullLogger<Ledger>.Instance);
            var manifest = await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken);
            var journal = JournalCoordinatorFactory.Create(options, manifest, manifestStore, new AsyncManualResetEvent(true), NullLoggerFactory.Instance, out _);
            return new Harness(manifestStore, journal, clock);
        }

        internal JournalLoggingCacheDecorator<string> CreateDecorator(ILogicalNamespacedCache<string> inner, bool useRawReader = false) =>
            new(Self, RocksDoubles.CreateOwnerLocator(Self), inner, Journal, new DurableMutationExecutor(Journal, NullLogger<DurableMutationExecutor>.Instance), _clock, useRawReader ? Physical.RawReader : null);
    }
}
