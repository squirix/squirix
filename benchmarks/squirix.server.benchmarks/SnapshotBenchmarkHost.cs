using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.Benchmarks;

internal sealed class SnapshotBenchmarkHost : IDisposable
{
    private readonly TempDirectory _dir;
    private readonly IReadOnlyList<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)> _items;
    private readonly ISnapshotWriter _writer;
    private int _nextIndex;

    private SnapshotBenchmarkHost(TempDirectory dir, PersistenceOptions options, IReadOnlyList<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)> items)
    {
        _dir = dir;
        _items = items;
        _writer = StoreFactory.CreateWriter(options);
        Reader = StoreFactory.CreateReader();
    }

    internal ISnapshotReader Reader { get; }

    public void Dispose() => _dir.Dispose();

    internal static Task<SnapshotBenchmarkHost> CreateAsync(string tempDirectoryPrefix, PersistenceOptions options, int entryCount)
    {
        ArgumentException.ThrowIfNullOrEmpty(tempDirectoryPrefix);
        ArgumentNullException.ThrowIfNull(options);

        var dir = new TempDirectory(tempDirectoryPrefix);
        var persistence = options with { DataDir = dir };
        var items = new List<(CacheKey Key, NodeCacheEntry<StoredValue> Entry)>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            var value = (i % 3) switch
            {
                0 => StoredValue.FromText($"value-{NodeInvariantIndexStrings.Format(i)}"),
                1 => StoredValue.FromInteger(i),
                _ => StoredValue.FromDouble(i * 1.5d),
            };
            items.Add((CacheKey.Default($"key-{NodeInvariantIndexStrings.Format(i)}"), new NodeCacheEntry<StoredValue> { Value = value, Version = 1 }));
        }

        return Task.FromResult(new SnapshotBenchmarkHost(dir, persistence, items));
    }

    internal ValueTask<string> WriteNextSnapshotAsync()
    {
        _nextIndex++;
        return _writer.WriteAsync(_nextIndex, _items, [], CancellationToken.None);
    }
}
