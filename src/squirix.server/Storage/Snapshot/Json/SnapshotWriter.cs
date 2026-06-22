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
using Squirix.Server.Storage.Journaling.Entries;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot.Json;

internal sealed class SnapshotWriter : ISnapshotWriter
{
    private readonly string _dataDir;
    private readonly IStorageFileOperations _fileOperations;

    public SnapshotWriter(string dataDir)
        : this(dataDir, new StorageFileOperations())
    {
    }

    internal SnapshotWriter(string dataDir, IStorageFileOperations fileOperations)
    {
        _dataDir = dataDir;
        _fileOperations = fileOperations;
    }

    public async Task<string> WriteAsync(
        int index,
        IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> items,
        IEnumerable<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var tmp = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Snapshot}{index.ToString("000000", CultureInfo.InvariantCulture)}.tmp");
        try
        {
            await WriteSnapshotTempFileAsync(tmp, items, idempotencyRecords, cancellationToken).ConfigureAwait(false);
            var snap = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Snapshot}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Snapshot}");
            return _fileOperations.PublishSnapshot(tmp, snap) ? snap : throw new IOException($"Failed to publish snapshot to '{snap}'.");
        }
        finally
        {
            _ = FileEx.TryDeleteFile(tmp);
        }
    }

    private static async IAsyncEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> ToAsync(
        IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> items,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var it in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return it;
            await Task.Yield();
        }
    }

    private static async IAsyncEnumerable<PersistedIdempotencyRecord> ToAsync(
        IEnumerable<PersistedIdempotencyRecord> items,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var it in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return it;
            await Task.Yield();
        }
    }

    private static async Task WriteSnapshotTempFileAsync(
        string tmp,
        IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> items,
        IEnumerable<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        var fs = new FileStream(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, FileOptions.Asynchronous);
        await using (fs.ConfigureAwait(false))
        {
            await foreach (var (key, entry) in ToAsync(items, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var entryJson = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(entry.Value, entry.ExpiresUtc, entry.Expiration, entry.Version, entry.Tags)
                                                                  .ConfigureAwait(false);
                using var entryDoc = JsonDocument.Parse(entryJson);
                var json = SerializationProvider.Instance.SerializeToUtf8Bytes(
                    new SnapshotFrame
                    {
                        Kind = "entry",
                        Namespace = key.Namespace,
                        Key = key.Key,
                        Entry = entryDoc.RootElement.Clone(),
                    });
                await FrameCodec.WriteFrameAsync(fs, json, cancellationToken).ConfigureAwait(false);
            }

            await foreach (var record in ToAsync(idempotencyRecords, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var json = JsonSerializer.SerializeToUtf8Bytes(
                    new SnapshotFrame { Kind = "idempotency", Idempotency = record },
                    SquirixJsonSerializerContext.Default.SnapshotFrame);
                await FrameCodec.WriteFrameAsync(fs, json, cancellationToken).ConfigureAwait(false);
            }

            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
