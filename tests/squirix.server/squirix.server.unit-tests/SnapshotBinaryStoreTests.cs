using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Binary snapshot writer/reader integration tests.</summary>
[Immutable]
public sealed class SnapshotBinaryStoreTests : ServerUnitTestBase
{
    private static readonly byte[] SampleBytes = [1, 2, 3];

    /// <summary>Streaming reader rejects snapshots with a corrupted file CRC footer.</summary>
    [Fact]
    public async Task LoadStrictAsyncRejectsCorruptedFileCrc()
    {
        using var dir = new TempDirectory("squirix-binary-snapshot-crc");
        var options = new PersistenceOptions { DataDir = dir.Path };
        var writer = StoreFactory.CreateWriter(options);
        var reader = StoreFactory.CreateReader(options);
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>
        {
            (CacheKey.Default("k"), new NodeCacheEntry<object?> { Value = "v", Version = 1 }),
        };

        var path = await writer.WriteAsync(1, items, [], DefaultCancellationToken);
        var bytes = await File.ReadAllBytesAsync(path, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, DefaultCancellationToken);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(reader, path, static (r, p) => _ = r.LoadStrictAsync<object?>(p, cancellationToken: DefaultCancellationToken));
    }

    /// <summary>Writes mixed entries and idempotency records, then loads them back.</summary>
    [Fact]
    public async Task WriteAndReadRoundTripMixedEntries()
    {
        using var dir = new TempDirectory("squirix-binary-snapshot");
        var options = new PersistenceOptions { DataDir = dir.Path };
        var writer = StoreFactory.CreateWriter(options);
        var reader = StoreFactory.CreateReader(options);

        var items = BuildSampleItems();
        var idempotency = BuildIdempotencyRecords();

        var path = await writer.WriteAsync(1, items, idempotency, DefaultCancellationToken);
        Assert.EndsWith(".bsqx", path, StringComparison.Ordinal);
        Assert.True(File.Exists(path));

        var loaded = await reader.LoadStrictAsync<object?>(path, cancellationToken: DefaultCancellationToken);
        Assert.Equal(items.Count, loaded.Entries.Count);
        Assert.Equal(idempotency.Length, loaded.IdempotencyRecords.Count);

        var byKey = ToDictionary(loaded.Entries);
        foreach (var (key, entry) in items)
        {
            var lookupKey = $"{key.Namespace}:{key.Key}";
            Assert.True(byKey.TryGetValue(lookupKey, out var roundTrip));
            Assert.True(EntryEquals(entry, roundTrip));
        }
    }

    private static PersistedIdempotencyRecord[] BuildIdempotencyRecords() =>
    [
        new("op-parity", "fp-parity", new TryAddAsyncResponse { Added = true }.ToByteArray(), new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc)),
    ];

    private static List<(CacheKey Key, NodeCacheEntry<object?> Entry)> BuildSampleItems() =>
    [
        (CacheKey.Default("alpha"), new NodeCacheEntry<object?> { Value = "text", Version = 2 }),
        (CacheKey.Default("beta"), new NodeCacheEntry<object?> { Value = 42L, Version = 3, Expiration = TimeSpan.FromMinutes(1) }),
        (CacheKey.Default("gamma"), new NodeCacheEntry<object?> { Value = 3.5d, Version = 1, ExpiresUtc = new DateTime(2026, 12, 1, 0, 0, 0, DateTimeKind.Utc) }),
        (new CacheKey("ns", "bytes"), new NodeCacheEntry<object?>(SampleBytes, 4, tags: EntryTagsKit.RegionWest)),
    ];

    private static bool EntryEquals(NodeCacheEntry<object?> left, NodeCacheEntry<object?> right)
    {
        if (left.Version != right.Version)
            return false;

        if (left.ExpiresUtc != right.ExpiresUtc)
            return false;

        if (left.Expiration != right.Expiration)
            return false;

        return ValueEquals(left.Value, right.Value) && TagsEqual(left.Tags, right.Tags);
    }

    private static bool TagsEqual(FrozenDictionary<string, string>? left, FrozenDictionary<string, string>? right)
    {
        if (left == null && right == null)
            return true;

        if (left == null || right == null || left.Count != right.Count)
            return false;

        foreach (var pair in left)
        {
            if (!right.TryGetValue(pair.Key, out var other) || !string.Equals(pair.Value, other, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static SortedDictionary<string, NodeCacheEntry<object?>> ToDictionary(List<(CacheKey Key, NodeCacheEntry<object?> Entry)> entries)
    {
        var result = new SortedDictionary<string, NodeCacheEntry<object?>>(StringComparer.Ordinal);
        foreach (var (key, entry) in entries)
            result[$"{key.Namespace}:{key.Key}"] = entry;

        return result;
    }

    private static bool ValueEquals(object? left, object? right) => left switch
    {
        null => right == null,
        byte[] leftBytes when right is byte[] rightBytes => leftBytes.AsSpan().SequenceEqual(rightBytes),
        int i when right is long l => i == l,
        long l when right is int i => l == i,
        double d when right is double r => Math.Abs(d - r) < 0.0001,
        _ => Equals(left, right),
    };
}
