using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.Utils;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Builds snapshot files and reduces the result of a load, including its failure, to a comparable string.</summary>
internal static class LoadSupport
{
    internal const int LargeValueLength = 40_000;

    internal const int SmallLargeValueLength = 300;

    internal static readonly DateTime FixedUtc = new(2030, 1, 2, 3, 4, 5, DateTimeKind.Utc);

    internal static readonly DateTime LoadUtc = new(2030, 6, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>Builds a file from raw records, a valid header and a valid footer.</summary>
    /// <param name="records">The encoded records.</param>
    /// <returns>The file bytes.</returns>
    internal static byte[] AssembleFile(params byte[][] records)
    {
        using var stream = new MemoryStream();
        Span<byte> header = stackalloc byte[SnapshotCodec.FileHeaderSize];
        SnapshotCodec.WriteFileHeader(header);
        stream.Write(header);
        var crc = Crc32C.Append(Crc32C.InitialValue, SnapshotCodec.Version);
        foreach (var record in records)
        {
            stream.Write(record);
            crc = Crc32C.Append(crc, record);
        }

        Span<byte> footer = stackalloc byte[SnapshotCodec.FileFooterSize];
        BinaryPrimitives.WriteUInt32LittleEndian(footer, Crc32C.Finalize(crc));
        stream.Write(footer);
        return stream.ToArray();
    }

    /// <summary>Builds a few idempotency records.</summary>
    /// <returns>The records.</returns>
    internal static List<PersistedIdempotencyRecord> BuildIdempotency() =>
    [
        new("op-1", "fingerprint-1", [1, 2, 3, 4], FixedUtc),
        new("op-оп", null, FixedUtc),
    ];

    /// <summary>Builds the items of a mixed snapshot: short keys, JSON, tags, expired and live deadlines and one large value.</summary>
    /// <param name="smallCount">The number of small entries.</param>
    /// <param name="largeValueLength">The length of the large value.</param>
    /// <returns>The items.</returns>
    internal static List<(CacheKey Key, NodeCacheEntry<object?> Entry)> BuildItems(int smallCount, int largeValueLength)
    {
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(smallCount + 8);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["t1"] = "a", ["тег"] = "é" }.ToFrozenDictionary();
        for (var i = 0; i < smallCount; i++)
        {
            DateTime? deadline = (i % 5) switch
            {
                0 => FixedUtc.AddYears(-1),
                1 => FixedUtc.AddYears(1),
                _ => null,
            };
            items.Add((CacheKey.Default($"key-{i}"), new NodeCacheEntry<object?>($"value-{i}", i + 1L, deadline, null, i % 7 == 0 ? tags : null)));
        }

        items.Add((CacheKey.Default("json-object"), new NodeCacheEntry<object?> { Value = ParseJson("""{"a":1,"b":[true,null,"x"],"c":{"d":2.5}}"""), Version = 1 }));
        items.Add((CacheKey.Default("large"), new NodeCacheEntry<object?> { Value = new string('x', largeValueLength), Version = 1 }));
        items.Add((CacheKey.Default("ключ-日本"), new NodeCacheEntry<object?>("non-ascii", 2, FixedUtc, TimeSpan.FromMinutes(5), tags)));
        items.Add((CacheKey.Default("bytes"), new NodeCacheEntry<object?> { Value = new byte[] { 9, 8, 7 }, Version = 1 }));
        items.Add((CacheKey.Default("tail"), new NodeCacheEntry<object?> { Value = "tail", Version = 1 }));
        return items;
    }

    /// <summary>Loads the file and returns either the re-encoded content or the failure type and message.</summary>
    /// <param name="reader">The reader under test.</param>
    /// <param name="path">The snapshot path.</param>
    /// <param name="expiredAsOf">The expiry cut-off, or <see langword="null" />.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A string that is equal for equal outcomes.</returns>
    internal static async Task<string> DescribeLoadAsync(ISnapshotReader reader, string path, DateTime? expiredAsOf, CancellationToken cancellationToken)
    {
        try
        {
            var loaded = await reader.LoadStrictAsync<object?>(path, expiredAsOf, cancellationToken);
            var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(loaded.Entries);
            var rewritten = await WriteAsync(Path.GetDirectoryName(path)!, items, loaded.IdempotencyRecords, cancellationToken);
            var lengths = new StringBuilder();
            foreach (var length in loaded.EncodedLengths)
                _ = lengths.Append(length?.ToString(CultureInfo.InvariantCulture) ?? "-").Append(',');

            return $"OK|{Convert.ToBase64String(rewritten)}|{lengths}";
        }
        catch (Exception exception) when (exception is InvalidDataException or EndOfStreamException or UnsupportedSnapshotVersionException)
        {
            return Describe(exception);
        }
    }

    /// <summary>Writes a snapshot with the production writer and returns its bytes.</summary>
    /// <param name="dir">The directory the writer publishes into.</param>
    /// <param name="items">The entries.</param>
    /// <param name="idempotency">The idempotency records.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The file bytes.</returns>
    internal static async Task<byte[]> WriteAsync(string dir, IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items, IReadOnlyList<PersistedIdempotencyRecord> idempotency, CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(dir);
        var written = await writer.WriteAsync(writer.AllocateIndex(0), items, idempotency, cancellationToken);
        try
        {
            return await File.ReadAllBytesAsync(written, cancellationToken);
        }
        finally
        {
            File.Delete(written);
        }
    }

    private static string Describe(Exception exception) => $"{exception.GetType().FullName}|{exception.Message}";

    private static JsonElement ParseJson(string json)
    {
        using var document = JsonDocument.Parse(json);
        return document.RootElement.Clone();
    }
}
