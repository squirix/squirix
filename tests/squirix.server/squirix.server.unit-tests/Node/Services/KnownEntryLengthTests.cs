using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A JSON value is accounted as the walk of the stored entry would measure it on every path that already knows the encoded entry length:
/// the durable write chain, the replicated apply, and the recovery of the journal and of a snapshot.
/// </summary>
public sealed class KnownEntryLengthTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";
    private const long MeasuredBytes = 7777L;
    private static readonly JsonElement Replacement = JsonElement.Parse("""[{"k":"a much longer replacement value"},[1,2,3],"tail"]""");

    private static readonly JsonElement Tree = JsonElement.Parse("""{"a":[1,"x",true,null,{"b":2.5}],"name":"héllo","e":{}}""");

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-known-entry-length";

    /// <summary>The durable write and the replicated apply of an entry without a relative expiration reach the estimator with a length and never ask it to measure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChainNeverAsksEstimatorToMeasure(CancellationToken cancellationToken)
    {
        var clock = JournalReplayKit.CreateWriteClock(TimeSpan.Zero);
        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1 };
        var expectations = new ICacheEntrySizeEstimatorCreateExpectations<object?>();
        _ = expectations.Setups.EstimateBytesForEncodedLength(Arg.Any<CacheKey>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<int>()).Callback(static (_, _, length) => length);
        var leader = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        var leaderCache = new ClientCache<object?>(leader, leader);
        var factory = new ReplicaMutationFactory(leaderCache, "g1", 1UL, clock, NullLogger.Instance);
        var records = new List<ReplicaLogRecord>();
        var deadline = clock.GetUtcNow().UtcDateTime.AddHours(1);
        await LeaderAppliesAsync(
            leaderCache,
            records,
            factory.PrepareSet("op-1", CacheName, "applied", new NodeCacheEntry<object?>(Tree, 1, deadline, null, Tags()), 1UL),
            cancellationToken);

        await using var chain = await Chain.OpenAsync(persistence, clock, expectations.Instance(), cancellationToken);
        await chain.Cache.SetEntryAsync(string.Empty, CacheName, "written", new NodeCacheEntry<object?>(Tree, 1, deadline, null, Tags()), cancellationToken);
        _ = await chain.Cache.TryAddEntryAsync(string.Empty, CacheName, "added", new NodeCacheEntry<object?>(Replacement), cancellationToken);
        foreach (var record in records)
            await ReplicaCacheApplier.ApplyAsync(chain.Cache, in record, cancellationToken);

        long expected = 0;
        await foreach (var (_, entry) in chain.Physical.EnumerateLiveAsync(cancellationToken))
            expected += JournalEntryPayload.MeasureSerializedBytes(entry);

        _ = await Assert.That(CountOf(chain.Physical)).IsEqualTo(3);
        _ = await Assert.That(BytesOf(chain.Physical)).IsEqualTo(expected);
    }

    /// <summary>A decoded payload reports the length of the entry as stored with the deadline of the record, whatever deadline fields the payload holds.</summary>
    [Test]
    public async Task DecodedPayloadReportsStoredLength()
    {
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var withDeadline = JournalEntryPayloadKit.Encode(new NodeCacheEntry<object?>(Tree, 3, deadline, null, Tags()));
        var withoutDeadline = JournalEntryPayloadKit.Encode(new NodeCacheEntry<object?>(Tree, 3, null, null, Tags()));
        var relative = WithExpiration(withoutDeadline, 1);
        var both = WithExpiration(withDeadline, 9);

        foreach (var (payload, ticks) in new[]
                 {
                     (withDeadline, 0L), (withDeadline, deadline.Ticks), (withoutDeadline, 0L), (withoutDeadline, deadline.Ticks),
                     (relative, 0L), (relative, deadline.Ticks), (both, 0L), (both, deadline.Ticks),
                 })
        {
            var isDecoded = JournalEntryPayload.TryDecodeWithDeadline(payload, ticks, out var entry, out var length);

            _ = await Assert.That(isDecoded).IsTrue();
            _ = await Assert.That(length).IsEqualTo(JournalEntryPayload.MeasureSerializedBytes(entry!));
        }

        var padded = new byte[withDeadline.Length + 1];
        withDeadline.CopyTo(padded, 0);
        _ = await Assert.That(JournalEntryPayload.TryDecodeWithDeadline(padded, deadline.Ticks, out _, out var paddedLength)).IsTrue();
        _ = await Assert.That(paddedLength).IsNull();
    }

    /// <summary>
    /// An object with duplicate property names keeps them when it is decoded, so the replicated apply stores, journals and accounts the entry the
    /// leader encoded.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DuplicatePropertyNamesAreKept(CancellationToken cancellationToken)
    {
        var clock = JournalReplayKit.CreateWriteClock(TimeSpan.Zero);
        var estimator = new ObjectCacheEntrySizeEstimator();
        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1 };
        var duplicated = JsonElement.Parse("""{"a":1,"b":"some text","a":2}""");
        var leader = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        var leaderCache = new ClientCache<object?>(leader, leader);
        var factory = new ReplicaMutationFactory(leaderCache, "g1", 1UL, clock, NullLogger.Instance);
        var records = new List<ReplicaLogRecord>();
        await LeaderAppliesAsync(
            leaderCache,
            records,
            factory.PrepareSet("op-1", CacheName, "dup", new NodeCacheEntry<object?>(duplicated, 2, null, null, Tags()), 1UL),
            cancellationToken);

        long accounted;
        long walked;
        await using (var chain = await Chain.OpenAsync(persistence, clock, estimator, cancellationToken))
        {
            foreach (var record in records)
                await ReplicaCacheApplier.ApplyAsync(chain.Cache, in record, cancellationToken);

            var stored = await chain.Physical.GetEntryAsync(new CacheKey(CacheName, "dup"), cancellationToken);
            var json = stored!.Value is JsonUtf8Value found ? found : ThrowHelper.Throw<JsonUtf8Value>(new InvalidOperationException("The stored value must be a JSON value."));
            _ = await Assert.That(json.ToJsonElement().GetRawText()).IsEqualTo("""{"a":1,"b":"some text","a":2}""");
            accounted = BytesOf(chain.Physical);
            walked = await WalkedBytesAsync(chain.Physical, estimator, cancellationToken);
        }

        var recovered = await new JournalReplayKit(Dir, _testMeter).RecoverAsync(clock, estimator, cancellationToken);

        _ = await Assert.That(accounted).IsEqualTo(walked);
        _ = await Assert.That(BytesOf(recovered)).IsEqualTo(walked);
    }

    /// <summary>Set, add, update, touch and expiration removal through the durable chain account the walked bytes, and the journal recovers the same bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DurableChainAccountsWalkedBytes(CancellationToken cancellationToken)
    {
        var clock = JournalReplayKit.CreateWriteClock(TimeSpan.Zero);
        var estimator = new ObjectCacheEntrySizeEstimator();
        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1 };
        long accounted;
        long walked;
        int count;
        await using (var chain = await Chain.OpenAsync(persistence, clock, estimator, cancellationToken))
        {
            var cache = chain.Cache;
            var deadline = clock.GetUtcNow().UtcDateTime.AddHours(1);
            await cache.SetEntryAsync(string.Empty, CacheName, "plain", new NodeCacheEntry<object?>(Tree), cancellationToken);
            await cache.SetEntryAsync(string.Empty, CacheName, "tagged", new NodeCacheEntry<object?>(Tree, 3, deadline, null, Tags()), cancellationToken);
            await cache.SetEntryAsync(string.Empty, CacheName, "relative", new NodeCacheEntry<object?>(Tree, expiration: TimeSpan.FromMinutes(5)), cancellationToken);
            _ = await cache.TryAddEntryAsync(string.Empty, CacheName, "added", new NodeCacheEntry<object?>(Tree, 1, deadline, null, Tags()), cancellationToken);
            _ = await cache.TryAddEntryAsync(string.Empty, CacheName, "added-relative", new NodeCacheEntry<object?>(Tree, expiration: TimeSpan.FromMinutes(5)), cancellationToken);
            _ = await cache.TryAddEntryAsync(string.Empty, CacheName, "added", new NodeCacheEntry<object?>(Replacement), cancellationToken);
            _ = await cache.UpdateAsync(string.Empty, CacheName, "tagged", Replacement, cancellationToken);
            _ = await cache.TouchAsync(string.Empty, CacheName, "plain", TimeSpan.FromHours(2), cancellationToken);
            _ = await cache.RemoveExpirationAsync(string.Empty, CacheName, "added", cancellationToken);
            accounted = BytesOf(chain.Physical);
            walked = await WalkedBytesAsync(chain.Physical, estimator, cancellationToken);
            count = CountOf(chain.Physical);
        }

        var recovered = await new JournalReplayKit(Dir, _testMeter).RecoverAsync(clock, estimator, cancellationToken);

        _ = await Assert.That(count).IsEqualTo(5);
        _ = await Assert.That(accounted).IsEqualTo(walked);
        _ = await Assert.That(CountOf(recovered)).IsEqualTo(count);
        _ = await Assert.That(BytesOf(recovered)).IsEqualTo(walked);
    }

    /// <summary>A set, an add and a recovery insert with a known encoded length hand it to the estimator instead of having the entry measured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task KnownLengthReachesEstimator(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, CreateLengthEstimator());
        var keyA = new CacheKey(CacheName, "a");
        var keyB = new CacheKey(CacheName, "b");

        await cache.SetAsync(keyA, new NodeCacheEntry<string>("a", 1, new DateTime(2099, 1, 1, 0, 0, 0, DateTimeKind.Utc)), cancellationToken, 1000);
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(1000L);

        _ = await Assert.That(await cache.TryAddAsync(keyB, new NodeCacheEntry<string>("b"), cancellationToken, 20)).IsTrue();
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(1020L);

        await cache.InsertRecoveryAsync(keyA, new NodeCacheEntry<string>("c"), cancellationToken, 5);
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(25L);
    }

    /// <summary>A relative expiration becomes a deadline the known length may not cover, so the entry is measured and the known length is dropped.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RelativeExpirationIgnoresKnownLength(CancellationToken cancellationToken)
    {
        var cache = new PhysicalCache<string>(TimeProvider.System, CreateLengthEstimator());
        var relative = new NodeCacheEntry<string>("v", expiration: TimeSpan.FromMinutes(5));

        await cache.SetAsync(new CacheKey(CacheName, "a"), relative, cancellationToken, 1000);
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(MeasuredBytes);

        _ = await Assert.That(await cache.TryAddAsync(new CacheKey(CacheName, "b"), relative, cancellationToken, 1000)).IsTrue();
        _ = await Assert.That(BytesOf(cache)).IsEqualTo(2 * MeasuredBytes);
    }

    /// <summary>A replicated record, applied the way the leader and a follower apply it, accounts the walked bytes and journals the entry it decoded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicaApplyAccountsWalkedBytes(CancellationToken cancellationToken)
    {
        var clock = JournalReplayKit.CreateWriteClock(TimeSpan.Zero);
        var estimator = new ObjectCacheEntrySizeEstimator();
        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1 };
        var leader = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        var leaderCache = new ClientCache<object?>(leader, leader);
        var factory = new ReplicaMutationFactory(leaderCache, "g1", 1UL, clock, NullLogger.Instance);
        var deadline = clock.GetUtcNow().UtcDateTime.AddHours(1);
        var records = new List<ReplicaLogRecord>();
        var index = 1UL;
        await LeaderAppliesAsync(leaderCache, records, factory.PrepareSet("op-1", CacheName, "plain", new NodeCacheEntry<object?>(Tree), index++), cancellationToken);
        await LeaderAppliesAsync(
            leaderCache,
            records,
            factory.PrepareSet("op-2", CacheName, "tagged", new NodeCacheEntry<object?>(Tree, 3, deadline, null, Tags()), index++),
            cancellationToken);
        await LeaderAppliesAsync(
            leaderCache,
            records,
            factory.PrepareSet("op-3", CacheName, "relative", new NodeCacheEntry<object?>(Tree, expiration: TimeSpan.FromMinutes(5)), index++),
            cancellationToken);
        await LeaderAppliesAsync(
            leaderCache,
            records,
            await factory.PrepareTryAddAsync("op-4", CacheName, "added", new NodeCacheEntry<object?>(Tree, 1, deadline), index++, cancellationToken),
            cancellationToken);
        await LeaderAppliesAsync(leaderCache, records, await factory.PrepareUpdateAsync("op-5", CacheName, "tagged", Replacement, index++, cancellationToken), cancellationToken);
        await LeaderAppliesAsync(
            leaderCache,
            records,
            await factory.PrepareTouchAsync("op-6", CacheName, "plain", TimeSpan.FromHours(2), index, cancellationToken),
            cancellationToken);

        long accounted;
        long walked;
        await using (var chain = await Chain.OpenAsync(persistence, clock, estimator, cancellationToken))
        {
            foreach (var record in records)
                await ReplicaCacheApplier.ApplyAsync(chain.Cache, in record, cancellationToken);

            accounted = BytesOf(chain.Physical);
            walked = await WalkedBytesAsync(chain.Physical, estimator, cancellationToken);
            _ = await Assert.That(CountOf(chain.Physical)).IsEqualTo(4);
        }

        var recovered = await new JournalReplayKit(Dir, _testMeter).RecoverAsync(clock, estimator, cancellationToken);

        _ = await Assert.That(accounted).IsEqualTo(walked);
        _ = await Assert.That(BytesOf(recovered)).IsEqualTo(walked);
    }

    /// <summary>A snapshot entry reports the encoded length of the entry it holds, and replaying it accounts the walked bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotRecoveryAccountsWalkedBytes(CancellationToken cancellationToken)
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        (CacheKey Key, NodeCacheEntry<object?> Entry)[] items =
        [
            (new CacheKey(CacheName, "plain"), new NodeCacheEntry<object?>(Tree)),
            (new CacheKey(CacheName, "tagged"), new NodeCacheEntry<object?>(Replacement, 2, deadline, null, Tags())),
        ];
        var writer = StoreFactory.CreateWriter(new PersistenceOptions { DataDir = Dir });
        var path = await writer.WriteAsync(1, items, [], cancellationToken);

        var loaded = await StoreFactory.CreateReader().LoadStrictAsync<object?>(path, null, cancellationToken);

        var replayed = new PhysicalCache<object?>(TimeProvider.System, estimator);
        long expected = 0;
        for (var i = 0; i < loaded.Entries.Count; i++)
        {
            var (key, entry) = loaded.Entries[i];
            var length = loaded.EncodedLengths[i];
            _ = await Assert.That(length).IsEqualTo(JournalEntryPayload.MeasureSerializedBytes(entry));
            await replayed.InsertRecoveryAsync(key, entry, cancellationToken, length);
            expected += estimator.EstimateBytes(key, entry, false);
        }

        _ = await Assert.That(loaded.Entries.Count).IsEqualTo(items.Length);
        _ = await Assert.That(BytesOf(replayed)).IsEqualTo(expected);
    }

    /// <summary>A hint that is shorter or longer than the entry really encodes to is replaced by the measured length, and the frame is whole.</summary>
    [Test]
    public async Task WrongHintFallsBackToMeasuredLength()
    {
        var entry = new NodeCacheEntry<object?>(Tree, 3, new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc), null, Tags());
        var measured = JournalEntryPayload.MeasureSerializedBytes(entry);

        foreach (var hint in new[] { measured - 5, measured + 5, 1 })
        {
            var prepared = JournalEntryPayload.PrepareEncode(entry, hint);
            using var payload = JournalEntryPayload.Encode(in prepared);

            _ = await Assert.That(payload.Memory.Length).IsEqualTo(measured);
            var isDecoded = JournalEntryPayload.TryDecode<object?>(payload.Memory.Span, out var decoded, out var bytesRead);
            _ = await Assert.That(isDecoded).IsTrue();
            _ = await Assert.That(bytesRead).IsEqualTo(measured);
            _ = await Assert.That(JournalEntryPayload.MeasureSerializedBytes(decoded!)).IsEqualTo(measured);
        }
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static int CountOf(ILocalCacheStats stats) => stats.EntryCount;

    private static ICacheEntrySizeEstimator<string> CreateLengthEstimator()
    {
        var estimator = new ICacheEntrySizeEstimatorCreateExpectations<string>();
        _ = estimator.Setups.EstimateBytes(Arg.Any<CacheKey>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<bool>()).ReturnValue(MeasuredBytes);
        _ = estimator.Setups.EstimateBytesForEncodedLength(Arg.Any<CacheKey>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<int>()).Callback(static (_, _, length) => length);
        return estimator.Instance();
    }

    private static async Task LeaderAppliesAsync(ClientCache<object?> cache, List<ReplicaLogRecord> records, PreparedReplicaMutation prepared, CancellationToken cancellationToken)
    {
        var record = ReplicaLogCodec.Decode(prepared.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record must decode."));
        await ReplicaCacheApplier.ApplyAsync(cache, in record, cancellationToken);
        records.Add(record);
    }

    private static FrozenDictionary<string, string> Tags() =>
        new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "t1", ["tier"] = "gold" }.ToFrozenDictionary(StringComparer.Ordinal);

    private static async Task<long> WalkedBytesAsync(PhysicalCache<object?> cache, ObjectCacheEntrySizeEstimator estimator, CancellationToken cancellationToken)
    {
        long sum = 0;
        await foreach (var (key, entry) in cache.EnumerateLiveAsync(cancellationToken))
            sum += estimator.EstimateBytes(key, entry, false);

        return sum;
    }

    /// <summary>Turns on the relative-expiration field of an encoded entry and inserts its value; the field sits after the deadline field.</summary>
    /// <param name="payload">The encoded entry.</param>
    /// <param name="flagIndex">The index of the relative-expiration flag byte.</param>
    /// <returns>The encoded entry with a relative expiration.</returns>
    private static byte[] WithExpiration(byte[] payload, int flagIndex)
    {
        var result = new byte[payload.Length + sizeof(long)];
        payload.AsSpan(0, flagIndex).CopyTo(result);
        result[flagIndex] = 1;
        BinaryPrimitives.WriteInt64LittleEndian(result.AsSpan(flagIndex + 1), TimeSpan.FromMinutes(1).Ticks);
        payload.AsSpan(flagIndex + 1).CopyTo(result.AsSpan(flagIndex + 1 + sizeof(long)));
        return result;
    }

    /// <summary>The local write chain of a persistent host over a physical cache: payload preparation, journal logging, client cache.</summary>
    [Immutable]
    private sealed class Chain : IAsyncDisposable
    {
        private readonly IJournalCoordinator _journal;
        private readonly Ledger _manifestStore;

        private Chain(Ledger manifestStore, IJournalCoordinator journal, PhysicalCache<object?> physical, JournalPayloadPrepareCacheDecorator<object?> cache)
        {
            _manifestStore = manifestStore;
            _journal = journal;
            Physical = physical;
            Cache = cache;
        }

        internal JournalPayloadPrepareCacheDecorator<object?> Cache { get; }

        internal PhysicalCache<object?> Physical { get; }

        public async ValueTask DisposeAsync()
        {
            await _journal.DisposeAsync();
            _manifestStore.Dispose();
        }

        internal static async Task<Chain> OpenAsync(
            PersistenceOptions persistence,
            FakeTimeProvider clock,
            ICacheEntrySizeEstimator<object?> estimator,
            CancellationToken cancellationToken)
        {
            var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
            var journal = JournalCoordinatorFactory.Create(
                persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                manifestStore,
                new AsyncManualResetEvent(true),
                NullLoggerFactory.Instance,
                clock,
                out _);
            var physical = new PhysicalCache<object?>(clock, estimator);
            var logging = new JournalLoggingCacheDecorator<object?>(
                new ClientCache<object?>(physical, physical),
                journal,
                new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance),
                clock,
                physical.RawReader);
            return new Chain(manifestStore, journal, physical, new JournalPayloadPrepareCacheDecorator<object?>(logging));
        }
    }
}
