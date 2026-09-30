using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
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
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Pinned deadlines survive the local journal exactly: an entry recovered from the cache journal equals the entry a re-apply of the
/// same replicated records writes, deadline included.
/// </summary>
public sealed class ReplicaJournalPrecisionTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const string Self = "node-a";

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-replica-journal-precision";

    /// <summary>The journal keeps whole milliseconds, so every pinned deadline is a whole millisecond and journal recovery and log re-apply agree.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournalRecoveryEqualsLogReapply(CancellationToken cancellationToken)
    {
        // A prepare instant that is not on a whole millisecond, so an untruncated deadline would differ from the journaled one.
        var real = DateTimeOffset.UtcNow;
        var clock = new FakeTimeProvider(real.AddTicks(-(real.Ticks % TimeSpan.TicksPerMillisecond) + 1234));
        var records = await PrepareRecordsAsync(clock, cancellationToken);

        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 1, FlushInterval = 5, ManifestRetentionCount = 1 };
        await ApplyThroughJournalAsync(persistence, clock, records, cancellationToken);
        var recovered = await RecoverAsync(persistence, clock, cancellationToken);
        var direct = new PhysicalCache<object?>(clock);
        var directCache = new ClientCache<object?>(direct, direct);
        foreach (var record in records)
            await ReplicaCacheApplier.ApplyAsync(directCache, record, cancellationToken);

        var expected = await direct.RawReader.GetEntryRawAsync(new CacheKey(CacheName, Key), cancellationToken);
        var actual = await recovered.RawReader.GetEntryRawAsync(new CacheKey(CacheName, Key), cancellationToken);
        _ = await Assert.That(expected).IsNotNull();
        _ = await Assert.That(actual).IsNotNull();
        _ = await Assert.That(actual!.Value).IsEqualTo(expected!.Value);
        _ = await Assert.That(actual.ExpiresUtc).IsEqualTo(expected.ExpiresUtc);
        _ = await Assert.That(actual.Version).IsEqualTo(expected.Version);
        _ = await Assert.That(actual.Tags?["t"]).IsEqualTo(expected.Tags?["t"]);
        _ = await Assert.That(expected.ExpiresUtc!.Value.Ticks % TimeSpan.TicksPerMillisecond).IsEqualTo(0L);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private static async Task<List<ReplicaLogRecord>> PrepareRecordsAsync(FakeTimeProvider clock, CancellationToken cancellationToken)
    {
        var physical = new PhysicalCache<object?>(clock);
        var cache = new ClientCache<object?>(physical, physical);
        var factory = new ReplicaMutationFactory(cache, "g1", 1UL, clock);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["t"] = "1" }.ToFrozenDictionary(StringComparer.Ordinal);
        var records = new List<ReplicaLogRecord>();
        await LeaderAppliesAsync(cache, records, factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?>("v1", 3, null, TimeSpan.FromMinutes(10), tags), 1UL), cancellationToken);
        await LeaderAppliesAsync(cache, records, await factory.PrepareTouchAsync("op-2", CacheName, Key, TimeSpan.FromMinutes(20), 2UL, cancellationToken), cancellationToken);
        await LeaderAppliesAsync(cache, records, await factory.PrepareUpdateAsync("op-3", CacheName, Key, "v2", 3UL, cancellationToken), cancellationToken);
        return records;
    }

    private static async Task LeaderAppliesAsync(ClientCache<object?> cache, List<ReplicaLogRecord> records, PreparedReplicaMutation prepared, CancellationToken cancellationToken)
    {
        var record = ReplicaLogCodec.Decode(prepared.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record must decode."));
        await ReplicaCacheApplier.ApplyAsync(cache, record, cancellationToken);
        records.Add(record);
    }

    private static async Task ApplyThroughJournalAsync(PersistenceOptions persistence, FakeTimeProvider clock, List<ReplicaLogRecord> records, CancellationToken cancellationToken)
    {
        using var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            NullLogger.Instance);
        var physical = new PhysicalCache<object?>(clock);
        var cache = new JournalLoggingCacheDecorator<object?>(
            Self,
            RocksDoubles.CreateOwnerLocator(Self),
            new ClientCache<object?>(physical, physical),
            journal,
            new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance),
            clock);
        foreach (var record in records)
            await ReplicaCacheApplier.ApplyAsync(cache, record, cancellationToken);
    }

    private async Task<PhysicalCache<object?>> RecoverAsync(PersistenceOptions persistence, TimeProvider clock, CancellationToken cancellationToken)
    {
        using var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance);
        var cache = new PhysicalCache<object?>(clock);
        var dependencies = new RecoveryDependencies<object?>(
            persistence,
            manifestStore,
            cache,
            new AsyncManualResetEvent(true),
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter)),
            StoreFactory.CreateReader());
        await new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, dependencies).StartAsync(cancellationToken);
        return cache;
    }
}
