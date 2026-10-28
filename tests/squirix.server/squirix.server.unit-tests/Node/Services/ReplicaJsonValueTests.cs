using System;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
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
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A structured value keeps the exact bytes the client sent on the leader, on a follower applying the replicated record and after journal recovery.</summary>
public sealed class ReplicaJsonValueTests : IsolatedStorageTestBase
{
    private const string CacheName = "cache";
    private const string Key = "k";
    private const int WideObjectPropertyCount = 70_000;

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-replica-json-value";

    /// <summary>Numbers beyond the range of a long and of a double keep every digit on every path.</summary>
    /// <param name="text">The JSON text of the value.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("12345678901234567890")]
    [Arguments("1.2345678901234567890123")]
    [Arguments("""{"big":12345678901234567890,"precise":1.2345678901234567890123,"dup":1,"dup":2}""")]
    public async Task ValueKeepsBytesOnEveryPath(string text, CancellationToken cancellationToken)
    {
        var value = JsonValueKit.Create(text);

        var (leader, follower, recovered) = await ApplyEverywhereAsync(value, cancellationToken);

        _ = await Assert.That(leader).IsEqualTo(text);
        _ = await Assert.That(follower).IsEqualTo(text);
        _ = await Assert.That(recovered).IsEqualTo(text);
    }

    /// <summary>An object with more properties than a 16-bit count holds keeps its bytes on every path.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WideObjectKeepsBytesOnEveryPath(CancellationToken cancellationToken)
    {
        var builder = new StringBuilder("{");
        for (var i = 0; i < WideObjectPropertyCount; i++)
        {
            var number = i.ToString(CultureInfo.InvariantCulture);
            _ = builder.Append(i == 0 ? string.Empty : ",").Append("\"p").Append(number).Append("\":").Append(number);
        }

        var text = builder.Append('}').ToString();

        var (leader, follower, recovered) = await ApplyEverywhereAsync(JsonValueKit.Create(text), cancellationToken);

        _ = await Assert.That(string.Equals(leader, text, StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(string.Equals(follower, text, StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(string.Equals(recovered, text, StringComparison.Ordinal)).IsTrue();
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private static string TextOf(NodeCacheEntry<object?>? entry) =>
        entry?.Value is JsonUtf8Value json ? Encoding.UTF8.GetString(json.Bytes.Span) : "<not a JSON value>";

    private static async Task ApplyThroughJournalAsync(PersistenceOptions persistence, FakeTimeProvider clock, ReplicaLogRecord record, CancellationToken cancellationToken)
    {
        using var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true),
            NullLoggerFactory.Instance,
            clock,
            out _);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        var cache = new JournalLoggingCacheDecorator<object?>(
            new ClientCache<object?>(physical, physical),
            journal,
            new DurableMutationExecutor(journal, NullLogger<DurableMutationExecutor>.Instance),
            clock);
        await ReplicaCacheApplier.ApplyAsync(cache, in record, cancellationToken);
    }

    private async Task<(string Leader, string Follower, string Recovered)> ApplyEverywhereAsync(JsonUtf8Value value, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UtcNow);
        var key = new CacheKey(CacheName, Key);
        var leader = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        var leaderCache = new ClientCache<object?>(leader, leader);
        var factory = new ReplicaMutationFactory(leaderCache, "g1", 1UL, clock, NullLogger.Instance);
        var prepared = factory.PrepareSet("op-1", CacheName, Key, new NodeCacheEntry<object?>(value, 1), 1UL);
        var record = ReplicaLogCodec.Decode(prepared.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record must decode."));
        await ReplicaCacheApplier.ApplyAsync(leaderCache, in record, cancellationToken);

        var follower = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        await ReplicaCacheApplier.ApplyAsync(new ClientCache<object?>(follower, follower), in record, cancellationToken);

        var persistence = new PersistenceOptions { DataDir = Dir, JournalMaxSegmentMb = 8 };
        await ApplyThroughJournalAsync(persistence, clock, record, cancellationToken);
        var recovered = await RecoverAsync(persistence, clock, cancellationToken);

        return (
            TextOf(await leader.RawReader.GetEntryRawAsync(key, cancellationToken)),
            TextOf(await follower.RawReader.GetEntryRawAsync(key, cancellationToken)),
            TextOf(await recovered.RawReader.GetEntryRawAsync(key, cancellationToken)));
    }

    private async Task<PhysicalCache<object?>> RecoverAsync(PersistenceOptions persistence, TimeProvider clock, CancellationToken cancellationToken)
    {
        using var manifestStore = new Ledger(persistence, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance);
        var cache = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        var dependencies = new RecoveryDependencies<object?>(
            persistence,
            manifestStore,
            cache,
            new AsyncManualResetEvent(true),
            new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter), TimeProvider.System),
            StoreFactory.CreateReader(),
            clock);
        await new RecoveryService<object?>(new RecoveryOptions { BlockOnStart = true }, NullLogger<RecoveryService<object?>>.Instance, dependencies).StartAsync(cancellationToken);
        return cache;
    }
}
