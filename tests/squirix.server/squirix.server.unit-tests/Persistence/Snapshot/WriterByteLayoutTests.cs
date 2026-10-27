using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Pins the exact bytes of written snapshot files so batching and in-place sealing never change the on-disk format.</summary>
[Immutable]
public sealed class WriterByteLayoutTests : IsolatedStorageTestBase
{
    private const string EmptySnapshotSha256 = "C615E6B52415A3BC1BBE85AA2CDA87B179AC3D6DA93DE58F34A6FC1A9B1CFF15";

    private const string MixedSnapshotSha256 = "25824AAF76B3817148F65B8A1A95C4A0EAEA0E5F49207A0045000AE582A1A971";

    private const int LargeValueLength = 1536 * 1024;

    private const int SmallEntryCount = 25_000;

    private static readonly DateTime FixedUtc = new(2030, 1, 2, 3, 4, 5, DateTimeKind.Utc);

    /// <summary>An empty snapshot is exactly a header and a footer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptySnapshotBytesAreStable(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);

        var path = await writer.WriteAsync(1, [], [], cancellationToken);

        _ = await Assert.That(await HashAsync(path, cancellationToken)).IsEqualTo(EmptySnapshotSha256);
    }

    /// <summary>A mixed snapshot crossing many batch boundaries, with a record larger than the batch buffer, keeps its exact bytes and reads back.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MixedSnapshotBytesAreStable(CancellationToken cancellationToken)
    {
        var items = BuildMixedItems();
        var idempotency = new List<PersistedIdempotencyRecord>
        {
            new("op-1", "fingerprint-1", [1, 2, 3, 4], FixedUtc),
            new("op-оп", null, FixedUtc),
        };
        var writer = new SnapshotWriter(Dir);

        var path = await writer.WriteAsync(1, items, idempotency, cancellationToken);

        _ = await Assert.That(await HashAsync(path, cancellationToken)).IsEqualTo(MixedSnapshotSha256);
        var loaded = await StoreFactory.CreateReader().LoadStrictAsync<object?>(path, null, cancellationToken);
        _ = await Assert.That(loaded.Entries.Count).IsEqualTo(items.Count);
        _ = await Assert.That(loaded.IdempotencyRecords.Count).IsEqualTo(idempotency.Count);
    }

    private static List<(CacheKey Key, NodeCacheEntry<object?> Entry)> BuildMixedItems()
    {
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(SmallEntryCount + 8);
        for (var i = 0; i < SmallEntryCount; i++)
            items.Add((CacheKey.Default($"key-{i}"), new NodeCacheEntry<object?> { Value = $"value-{i}", Version = 1 }));

        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["t1"] = "a", ["тег"] = "é" }.ToFrozenDictionary();
        items.Add((CacheKey.Default("number"), new NodeCacheEntry<object?> { Value = 42, Version = 3 }));
        items.Add((CacheKey.Default("double"), new NodeCacheEntry<object?> { Value = 1.5d, Version = 1 }));
        items.Add((CacheKey.Default("ключ-日本"), new NodeCacheEntry<object?>("non-ascii", 2, FixedUtc, TimeSpan.FromMinutes(5), tags)));
        items.Add((CacheKey.Default("json-object"), new NodeCacheEntry<object?> { Value = ParseJson("""{"a":1,"b":[true,null,"x"],"c":{"d":2.5}}"""), Version = 1 }));
        items.Add((CacheKey.Default("json-array"), new NodeCacheEntry<object?> { Value = ParseJson("""[1,"two",{"three":3}]"""), Version = 1 }));
        items.Add((CacheKey.Default("large"), new NodeCacheEntry<object?> { Value = new string('x', LargeValueLength), Version = 1 }));
        items.Add((CacheKey.Default("bytes"), new NodeCacheEntry<object?> { Value = new byte[] { 9, 8, 7 }, Version = 1 }));
        items.Add((CacheKey.Default("tail"), new NodeCacheEntry<object?> { Value = "tail", Version = 1 }));
        return items;
    }

    private static async Task<string> HashAsync(string path, CancellationToken cancellationToken) => Convert.ToHexString(SHA256.HashData(await File.ReadAllBytesAsync(path, cancellationToken)));

    private static JsonElement ParseJson(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
}
