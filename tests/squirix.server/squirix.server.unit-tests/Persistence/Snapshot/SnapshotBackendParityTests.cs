using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;
using JsonSnapshotReader = Squirix.Server.Storage.Snapshot.Json.SnapshotReader;
using JsonSnapshotWriter = Squirix.Server.Storage.Snapshot.Json.SnapshotWriter;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Verifies JSON and binary snapshot backends produce equivalent cache entries.</summary>
public sealed class SnapshotBackendParityTests : UnitTestBase
{
    /// <summary>Equivalent entries round-trip through JSON and binary snapshot files.</summary>
    [Fact]
    public async Task JsonAndBinaryBackendsProduceEquivalentEntries()
    {
        using var dir = new TempDirectory("squirix-snapshot-parity");
        var items = BuildSampleItems();
        var idempotency = BuildIdempotencyRecords();

        var jsonPath = await new JsonSnapshotWriter(dir).WriteAsync(1, items, idempotency, DefaultCancellationToken);
        var binaryOptions = new PersistenceOptions { DataDir = dir, SnapshotBackend = SnapshotBackend.Binary };
        var binaryPath = await SnapshotStoreFactory.CreateWriter(binaryOptions).WriteAsync(1, items, idempotency, DefaultCancellationToken);

        var jsonLoaded = await new JsonSnapshotReader().LoadStrictAsync<object?>(jsonPath, cancellationToken: DefaultCancellationToken);
        var binaryLoaded = await SnapshotStoreFactory.CreateReader(binaryOptions).LoadStrictAsync<object?>(binaryPath, cancellationToken: DefaultCancellationToken);

        Assert.Equal(jsonLoaded.Entries.Count, binaryLoaded.Entries.Count);
        Assert.Equal(jsonLoaded.IdempotencyRecords.Count, binaryLoaded.IdempotencyRecords.Count);

        var jsonByKey = ToDictionary(jsonLoaded.Entries);
        var binaryByKey = ToDictionary(binaryLoaded.Entries);
        Assert.Equal(jsonByKey.Count, binaryByKey.Count);

        foreach (var key in jsonByKey.Keys)
        {
            Assert.True(EntryEquals(jsonByKey[key], binaryByKey[key]), $"Entry mismatch for key '{key}'.");
        }

        Assert.EndsWith(".ssqx", jsonPath, StringComparison.Ordinal);
        Assert.EndsWith(".bsqx", binaryPath, StringComparison.Ordinal);
        Assert.True(File.Exists(jsonPath));
        Assert.True(File.Exists(binaryPath));
    }

    private static List<(CacheKey Key, CacheEntry<object?> Entry)> BuildSampleItems() =>
    [
        (CacheKey.Default("alpha"), new CacheEntry<object?> { Value = "text", Version = 2 }),
        (CacheKey.Default("beta"), new CacheEntry<object?> { Value = 42L, Version = 3, Expiration = TimeSpan.FromMinutes(1) }),
        (CacheKey.Default("gamma"), new CacheEntry<object?> { Value = 3.5d, Version = 1, ExpiresUtc = new DateTime(2026, 12, 1, 0, 0, 0, DateTimeKind.Utc) }),
        (new CacheKey("ns", "bytes"), new CacheEntry<object?> { Value = new byte[] { 1, 2, 3 }, Version = 4, Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["region"] = "west" }.ToFrozenDictionary(StringComparer.Ordinal) }),
    ];

    private static PersistedIdempotencyRecord[] BuildIdempotencyRecords() =>
    [
        new()
        {
            OperationId = "op-parity",
            Fingerprint = "fp-parity",
            CreatedUtc = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
            Outcome = new PersistedIdempotencyOutcome { Kind = "insert" },
        },
    ];

    private static SortedDictionary<string, CacheEntry<object?>> ToDictionary(IReadOnlyList<(CacheKey Key, CacheEntry<object?> Entry)> entries)
    {
        var result = new SortedDictionary<string, CacheEntry<object?>>(StringComparer.Ordinal);
        foreach (var (key, entry) in entries)
            result[$"{key.Namespace}:{key.Key}"] = entry;

        return result;
    }

    private static bool EntryEquals(CacheEntry<object?> left, CacheEntry<object?> right)
    {
        if (left.Version != right.Version)
            return false;

        if (left.ExpiresUtc != right.ExpiresUtc)
            return false;

        if (left.Expiration != right.Expiration)
            return false;

        if (!ValueEquals(left.Value, right.Value))
            return false;

        if (left.Tags is null && right.Tags is null)
            return true;

        if (left.Tags is null || right.Tags is null || left.Tags.Count != right.Tags.Count)
            return false;

        foreach (var (tagKey, tagValue) in left.Tags)
        {
            if (!right.Tags.TryGetValue(tagKey, out var other) || !string.Equals(tagValue, other, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static bool ValueEquals(object? left, object? right) =>
        left switch
        {
            null => right is null,
            byte[] leftBytes when right is byte[] rightBytes => leftBytes.AsSpan().SequenceEqual(rightBytes),
            int i when right is long l => i == l,
            long l when right is int i => l == i,
            double d when right is double r => Math.Abs(d - r) < 0.0001,
            _ => Equals(left, right),
        };
}
