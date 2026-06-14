using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Storage.Snapshot;

internal sealed class SnapshotReader
{
    public static async Task<SnapshotLoadResult<T>> LoadStrictAsync<T>(string path, bool skipExpired = true, CancellationToken cancellationToken = default)
    {
        var entries = new List<(CacheKey Key, CacheEntry<T> Entry)>();
        var idempotencyRecords = new List<PersistedIdempotencyRecord>();

        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
        await using (fs.ConfigureAwait(false))
        {
            while (true)
            {
                var (ok, payload) = await FrameCodec.ReadFrameStrictAsync(fs, frame => ReadStrictPayload<T>(frame, skipExpired), cancellationToken).ConfigureAwait(false);
                if (!ok)
                    return new SnapshotLoadResult<T>(entries, idempotencyRecords);

                var snapshotPayload = payload ?? throw new InvalidDataException("Snapshot frame payload is missing.");
                if (snapshotPayload.Entry is { } entry)
                    entries.Add(entry);

                if (snapshotPayload.Idempotency is { } idempotency)
                    idempotencyRecords.Add(idempotency);
            }
        }
    }

    public static async IAsyncEnumerable<(CacheKey Key, CacheEntry<T> Entry)> ReadEntriesAsync<T>(
        string path,
        bool skipExpired = true,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 64 * 1024, FileOptions.Asynchronous);
        await using (fs.ConfigureAwait(false))
        {
            while (true)
            {
                var (ok, frame) = await FrameCodec.ReadFrameAsync(fs, payload => ReadEntryPayload<T>(payload, skipExpired), cancellationToken).ConfigureAwait(false);
                if (!ok)
                    yield break;

                if (frame is { HasEntry: true, Key: { } key, Entry: { } entry })
                    yield return (key, entry);
            }
        }
    }

    private static bool IsExpired(JsonElement entry)
    {
        if (!entry.TryGetProperty("expUtc", out var expiresUtcNode))
            return false;

        var expiresUtcValue = expiresUtcNode.GetString();
        return DateTime.TryParse(expiresUtcValue, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var expiresUtc) &&
               expiresUtc.ToUniversalTime() <= DateTime.UtcNow;
    }

    private static (bool HasEntry, CacheKey? Key, CacheEntry<T>? Entry) ReadEntryPayload<T>(ReadOnlyMemory<byte> payload, bool skipExpired)
    {
        using var doc = JsonDocument.Parse(payload, DurabilityJson.StrictDocumentOptions);
        var root = doc.RootElement;
        var kind = root.TryGetProperty("kind", out var kindNode) ? kindNode.GetString() ?? "entry" : "entry";

        if (!string.Equals(kind, "entry", StringComparison.Ordinal))
            return (false, null, null);

        var entryJson = root.GetProperty("entry");
        if ((skipExpired && IsExpired(entryJson)) || !DiscriminatedEntryJsonReader.TryElementToEntry<T>(entryJson, out var entry))
            return (false, null, null);

        var key = root.GetProperty("key").GetString()!;
        var cacheNamespace = root.TryGetProperty("namespace", out var namespaceNode) ? namespaceNode.GetString() : null;
        cacheNamespace = PersistedCacheNamespace.Normalize(cacheNamespace);
        return (true, new CacheKey(cacheNamespace, key), entry);
    }

    private static PersistedIdempotencyRecord? ReadIdempotencyPayload(ReadOnlyMemory<byte> payload, bool validate)
    {
        using var doc = JsonDocument.Parse(payload, DurabilityJson.StrictDocumentOptions);
        var root = doc.RootElement;
        var kind = root.TryGetProperty("kind", out var kindNode) ? kindNode.GetString() ?? "entry" : "entry";

        if (!string.Equals(kind, "idempotency", StringComparison.Ordinal))
            return null;

        var frame = JsonSerializer.Deserialize<SnapshotFrame>(payload.Span, DurabilityJson.StrictSerializerOptions) ??
                    throw new InvalidDataException("Snapshot idempotency frame could not be deserialized.");
        var record = frame.Idempotency ?? throw new InvalidDataException("Snapshot idempotency frame is missing payload.");
        if (validate)
            ValidateIdempotencyRecord(record);

        return record;
    }

    private static SnapshotPayload<T> ReadStrictEntryPayload<T>(JsonElement root, bool skipExpired)
    {
        ValidateSnapshotEntryMetadata(root);
        var entryJson = root.GetProperty("entry");
        if (skipExpired && IsExpired(entryJson))
            return new SnapshotPayload<T>(null, null);

        if (!DiscriminatedEntryJsonReader.TryElementToEntry<T>(entryJson, out var entry))
            throw new InvalidDataException("Snapshot entry payload could not be read.");

        var key = root.GetProperty("key").GetString();
        if (string.IsNullOrEmpty(key))
            throw new InvalidDataException("Snapshot entry key is missing.");

        var cacheNamespace = root.TryGetProperty("namespace", out var namespaceNode) ? namespaceNode.GetString() : null;
        cacheNamespace = PersistedCacheNamespace.Normalize(cacheNamespace);
        return new SnapshotPayload<T>((new CacheKey(cacheNamespace, key), entry), null);
    }

    private static SnapshotPayload<T> ReadStrictPayload<T>(ReadOnlyMemory<byte> payload, bool skipExpired)
    {
        using var doc = JsonDocument.Parse(payload, DurabilityJson.StrictDocumentOptions);
        var root = doc.RootElement;
        var kind = root.GetProperty("kind").GetString() ?? throw new InvalidDataException("Snapshot frame kind is missing.");

        return kind switch
        {
            "entry" => ReadStrictEntryPayload<T>(root, skipExpired),
            "idempotency" => new SnapshotPayload<T>(
                null,
                ReadIdempotencyPayload(payload, true) ?? throw new InvalidDataException("Snapshot idempotency frame is missing payload.")),
            _ => throw new InvalidDataException($"Unsupported snapshot frame kind: {kind}"),
        };
    }

    private static void ValidateIdempotencyRecord(PersistedIdempotencyRecord record)
    {
        if (string.IsNullOrWhiteSpace(record.OperationId))
            throw new InvalidDataException("Snapshot idempotency operation id is missing.");

        if (string.IsNullOrWhiteSpace(record.Fingerprint))
            throw new InvalidDataException("Snapshot idempotency fingerprint is missing.");

        if (!string.Equals(record.Outcome.Kind, "insert", StringComparison.Ordinal))
            throw new InvalidDataException($"Unsupported snapshot idempotency outcome kind: {record.Outcome.Kind}");
    }

    private static void ValidateSnapshotEntryMetadata(JsonElement root)
    {
        foreach (var property in root.EnumerateObject())
        {
            if (property.NameEquals("kind") || property.NameEquals("namespace") || property.NameEquals("key") || property.NameEquals("entry"))
            {
                continue;
            }

            throw new InvalidDataException($"Unsupported snapshot entry metadata field: {property.Name}");
        }
    }

    private sealed record SnapshotPayload<T>((CacheKey Key, CacheEntry<T> Entry)? Entry, PersistedIdempotencyRecord? Idempotency);
}
