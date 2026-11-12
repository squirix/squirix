using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Verifies measured snapshot entry bodies preserve their bytes and reject length mismatches.</summary>
[Immutable]
public sealed class EntryBodyEncodingTests : ServerUnitTestBase
{
    /// <summary>UTF-8 keys, values, tags and expiration metadata retain the existing binary encoding.</summary>
    /// <param name="large">Whether the value spans many kilobytes.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MeasuredBodyPreservesUtf8Bytes(bool large)
    {
        var key = new CacheKey("нс", "🔑");
        var value = large ? new string('я', 128 * 1024) + "🌍" : string.Empty;
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["тег"] = "日本🌍" }.ToFrozenDictionary();
        var entry = new NodeCacheEntry<object?>(value, 7, new DateTime(2030, 1, 2, 3, 4, 5, DateTimeKind.Utc), TimeSpan.FromMinutes(3), tags);
        byte[] keyBytes = [4, 0, 0xD0, 0xBD, 0xD1, 0x81, 4, 0, 0xF0, 0x9F, 0x94, 0x91];
        var expected = new byte[keyBytes.Length + CacheEntryCodec.ComputeEncodedLength(entry)];
        keyBytes.CopyTo(expected, 0);
        CacheEntryCodec.Write(entry, expected.AsSpan(keyBytes.Length));
        var actual = new byte[SnapshotCodec.ComputeEntryBodyLength(key, entry)];

        SnapshotCodec.WriteEntryBody(key, entry, actual);

        _ = await Assert.That(actual.AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(SnapshotCodec.TryReadEntryBody(actual, out var readKey, out var readEntry, out var encodedLength)).IsTrue();
        _ = await Assert.That(readKey).IsEqualTo(key);
        _ = await Assert.That(readEntry!.Value).IsEqualTo(value);
        _ = await Assert.That(readEntry.Version).IsEqualTo(entry.Version);
        _ = await Assert.That(readEntry.ExpiresUtc).IsEqualTo(entry.ExpiresUtc);
        _ = await Assert.That(readEntry.Expiration).IsEqualTo(entry.Expiration);
        _ = await Assert.That(readEntry.Tags!["тег"]).IsEqualTo(tags["тег"]);
        _ = await Assert.That(encodedLength).IsEqualTo(expected.Length - keyBytes.Length);
    }

    /// <summary>An oversized slot is rejected rather than including unwritten bytes in the snapshot body.</summary>
    [Test]
    public void BodyRejectsOversizedDestination()
    {
        var entry = new NodeCacheEntry<object?>("value");
        var destination = new byte[SnapshotCodec.ComputeEntryBodyLength(CacheKey.Default("key"), entry) + 1];

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            entry,
            destination,
            static (value, bytes) => SnapshotCodec.WriteEntryBody(CacheKey.Default("key"), value, bytes));
    }

    /// <summary>An undersized slot fails instead of writing outside the measured body.</summary>
    [Test]
    public void BodyRejectsUndersizedDestination()
    {
        var entry = new NodeCacheEntry<object?>("value");
        var destination = new byte[SnapshotCodec.ComputeEntryBodyLength(CacheKey.Default("key"), entry) - 1];

        _ = NodeExceptionAssert.For<ArgumentException>().Throws(
            entry,
            destination,
            static (value, bytes) => SnapshotCodec.WriteEntryBody(CacheKey.Default("key"), value, bytes));
    }
}
