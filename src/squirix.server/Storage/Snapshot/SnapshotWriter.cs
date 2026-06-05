using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Snapshot;

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

    public async Task<string> WriteAsync(int index, IEnumerable<(CacheKey Key, object Entry)> items, CancellationToken cancellationToken) =>
        await WriteAsync(index, items, [], cancellationToken).ConfigureAwait(false);

    public async Task<string> WriteAsync(
        int index,
        IEnumerable<(CacheKey Key, object Entry)> items,
        IEnumerable<PersistedIdempotencyRecord> idempotencyRecords,
        CancellationToken cancellationToken)
    {
        _ = DirectoryEx.CreateDirectory(_dataDir);

        var tmp = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Snapshot}{index:000000}.tmp");
        var moveCompleted = false;
        try
        {
            await using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, FileOptions.Asynchronous))
            {
                await foreach (var (k, e) in ToAsync(items, cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var json = SerializationProvider.Instance.SerializeToUtf8Bytes(new SnapshotFrame { Kind = "entry", Namespace = k.Namespace, Key = k.Key, Entry = e });
                    await FrameCodec.WriteFrameAsync(fs, json, cancellationToken).ConfigureAwait(false);
                }

                await foreach (var record in ToAsync(idempotencyRecords, cancellationToken))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var json = JsonSerializer.SerializeToUtf8Bytes(
                        new SnapshotFrame { Kind = "idempotency", Idempotency = record },
                        SquirixJsonSerializerContext.Default.SnapshotFrame);
                    await FrameCodec.WriteFrameAsync(fs, json, cancellationToken).ConfigureAwait(false);
                }

                await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
                fs.Flush(true);
            }

            var snap = PathEx.Combine(_dataDir, $"{StorageFilePrefixes.Snapshot}{index:000000}{StorageFileExtensions.Snapshot}");
            _fileOperations.PublishSnapshot(tmp, snap);
            moveCompleted = true;
            return snap;
        }
        finally
        {
            if (!moveCompleted)
                _ = FileEx.TryDeleteFile(tmp);
        }
    }

    private static async IAsyncEnumerable<(CacheKey Key, object Item)> ToAsync(
        IEnumerable<(CacheKey Key, object Item)> items,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var it in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return it;

            // Yield control back to the scheduler so long lists don't block the loop
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
}
