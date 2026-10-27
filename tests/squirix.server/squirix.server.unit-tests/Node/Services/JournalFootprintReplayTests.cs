using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A restart replays the journal into a cache that accounts the same bytes the cache held before.</summary>
[Immutable]
public sealed class JournalFootprintReplayTests : IsolatedStorageTestBase
{
    private const string CacheName = JournalReplayKit.CacheName;

    private readonly Meter _testMeter = new("test");

    /// <inheritdoc />
    protected override string TempDirectoryName => "squirix-journal-footprint-replay";

    private JournalReplayKit Kit => new(Dir, _testMeter);

    /// <summary>After sets, a replace, a touch and a remove, the recovered cache accounts the bytes the written cache held.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredCacheKeepsAccountedBytes(CancellationToken cancellationToken)
    {
        var clock = JournalReplayKit.CreateWriteClock(TimeSpan.Zero);
        long before;
        int entriesBefore;
        await using (var session = await Kit.OpenAsync(clock, cancellationToken))
        {
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "a", new NodeCacheEntry<string>("first", expiration: TimeSpan.FromHours(1)), cancellationToken);
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "b", new NodeCacheEntry<string>("second"), cancellationToken);
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "c", new NodeCacheEntry<string>("third"), cancellationToken);
            await session.Cache.SetEntryAsync(UnitMutationOpIds.Default, CacheName, "a", new NodeCacheEntry<string>("a much longer replacement", expiration: TimeSpan.FromHours(1)), cancellationToken);
            _ = await session.Cache.TouchAsync(UnitMutationOpIds.Default, CacheName, "b", TimeSpan.FromHours(1), cancellationToken);
            _ = await session.Cache.RemoveAsync(UnitMutationOpIds.Default, CacheName, "c", cancellationToken);
            before = BytesOf(session.Physical);
            entriesBefore = CountOf(session.Physical);
        }

        var recovered = await Kit.RecoverAsync(JournalReplayKit.CreateWriteClock(TimeSpan.Zero), cancellationToken);

        _ = await Assert.That(entriesBefore).IsEqualTo(2);
        _ = await Assert.That(before).IsGreaterThan(0L);
        _ = await Assert.That(CountOf(recovered)).IsEqualTo(entriesBefore);
        _ = await Assert.That(BytesOf(recovered)).IsEqualTo(before);
    }

    /// <summary>Integer and floating-point values, which the codec returns as Int64 and Double, account the same bytes after a replay as in memory before it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NumericValuesKeepAccountedBytes(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<object?>();
        var written = new PhysicalCache<object?>(TimeProvider.System, estimator);
        const short shortValue = 4;
        const byte byteValue = 5;
        object?[] values = [7, 3u, shortValue, byteValue, 1.5f, 2.5d, 9L];
        using (var manifestStore = new Ledger(Kit.Persistence, NullLogger<Ledger>.Instance, NoOpRetentionFailureMetrics.Instance))
        {
            await using var journal = JournalCoordinatorFactory.Create(
                Kit.Persistence,
                await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                manifestStore,
                new AsyncManualResetEvent(true),
                NullLoggerFactory.Instance,
                TimeProvider.System,
                out _);
            for (var i = 0; i < values.Length; i++)
            {
                var key = new CacheKey(CacheName, $"n{i}");
                var entry = new NodeCacheEntry<object?>(values[i]);
                await written.SetAsync(key, entry, cancellationToken);
                await journal.AppendPutUnderGateAsync(key, JournalEntryPayloadKit.Encode(entry), cancellationToken);
            }

            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var recovered = await Kit.RecoverAsync(TimeProvider.System, estimator, cancellationToken);

        _ = await Assert.That(CountOf(recovered)).IsEqualTo(values.Length);
        _ = await Assert.That(BytesOf(recovered)).IsEqualTo(BytesOf(written));
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        _testMeter.Dispose();
        base.DisposeManaged();
    }

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static int CountOf(ILocalCacheStats stats) => stats.EntryCount;
}
