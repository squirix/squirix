using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Entries.Binary;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Binary;

[SuppressMessage("Design", "MA0181:Do not use cast", Justification = "Error messages include the raw record kind byte.")]
internal sealed class SnapshotReader : ISnapshotReader
{
    public async Task<SnapshotLoadResult<T>> LoadStrictAsync<T>(string path, bool skipExpired = true, CancellationToken cancellationToken = default)
    {
        var entries = new List<(CacheKey Key, CacheEntry<T> Entry)>();
        var idempotencyRecords = new List<PersistedIdempotencyRecord>();
        await foreach (var record in ReadRecordsAsync(path, true, cancellationToken).ConfigureAwait(false))
        {
            switch (record)
            {
                case EntryRecord entry:
                    if (skipExpired && IsExpired(entry.Entry))
                        continue;

                    if (!CacheEntryCodec.TryMapEntry<T>(entry.Entry, out var mapped) || mapped is null)
                        throw new InvalidDataException("Binary snapshot entry payload could not be read.");

                    entries.Add((entry.Key, mapped));
                    break;

                case IdempotencyRecord idempotency:
                    idempotencyRecords.Add(idempotency.Record);
                    break;
            }
        }

        return new SnapshotLoadResult<T>(entries, idempotencyRecords);
    }

    private static bool IsExpired(CacheEntry<object?> entry) => entry.ExpiresUtc is { } expiresUtc && expiresUtc.ToUniversalTime() <= DateTime.UtcNow;

    private static async IAsyncEnumerable<object> ReadRecordsAsync(string path, bool strict, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        if (bytes.Length < Codec.FileHeaderSize + Codec.FileFooterSize)
            throw new InvalidDataException("Binary snapshot file is truncated.");

        Codec.ValidateFileHeader(bytes);
        var offset = Codec.FileHeaderSize;
        var crc = Crc32C.Append(Crc32C.InitialValue, [Codec.Version]);

        while (offset < bytes.Length - Codec.FileFooterSize)
        {
            if (!Codec.TryReadRecord(bytes.AsSpan(offset), out var kind, out var body, out var recordBytes))
            {
                if (strict)
                    throw new InvalidDataException("Binary snapshot record is truncated.");

                yield break;
            }

            crc = Crc32C.Append(crc, bytes.AsSpan(offset, recordBytes));
            offset += recordBytes;

            switch (kind)
            {
                case Codec.RecordKind.Entry:
                    if (!Codec.TryReadEntryBody(body, out var key, out var entry) || entry is null)
                    {
                        if (strict)
                            throw new InvalidDataException("Binary snapshot entry body could not be read.");

                        continue;
                    }

                    yield return new EntryRecord(key, entry);
                    break;

                case Codec.RecordKind.Idempotency:
                    if (TryReadIdempotency(body, strict, out var idempotencyRecord) && idempotencyRecord is not null)
                        yield return new IdempotencyRecord(idempotencyRecord);

                    break;

                default:
                    if (strict)
                        throw new InvalidDataException($"Unsupported binary snapshot record kind: {(byte)kind}.");

                    break;
            }
        }

        Codec.ValidateFileFooter(bytes, Crc32C.Finalize(crc));
    }

    private static bool TryReadIdempotency(ReadOnlySpan<byte> body, bool strict, out PersistedIdempotencyRecord? record)
    {
        try
        {
            record = IdempotencyCodec.Read(body);
            return true;
        }
        catch (InvalidDataException) when (!strict)
        {
            record = null;
            return false;
        }
    }

    private sealed record EntryRecord(CacheKey Key, CacheEntry<object?> Entry);

    private sealed record IdempotencyRecord(PersistedIdempotencyRecord Record);
}
