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

internal sealed class SnapshotBenchmarkHost : IAsyncDisposable
{
    private readonly TempDirectory _dataDir;
    private readonly IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> _items;
    private readonly ISnapshotWriter _writer;
    private int _nextIndex;

    private SnapshotBenchmarkHost(TempDirectory dataDir, PersistenceOptions options, IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)> items)
    {
        _dataDir = dataDir;
        _items = items;
        _writer = StoreFactory.CreateWriter(options);
        Reader = StoreFactory.CreateReader(options);
    }

    internal ISnapshotReader Reader { get; }

    public ValueTask DisposeAsync()
    {
        _dataDir.Dispose();
        return ValueTask.CompletedTask;
    }

    internal static Task<SnapshotBenchmarkHost> CreateAsync(string tempDirectoryPrefix, PersistenceOptions options, int entryCount)
    {
        ArgumentException.ThrowIfNullOrEmpty(tempDirectoryPrefix);
        ArgumentNullException.ThrowIfNull(options);

        var dataDir = new TempDirectory(tempDirectoryPrefix);
        var persistence = options with { DataDir = dataDir.Path };
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(entryCount);
        for (var i = 0; i < entryCount; i++)
        {
            object? value = (i % 3) switch
            {
                0 => $"value-{NodeInvariantIndexStrings.Format(i)}",
                1 => i,
                _ => i * 1.5d,
            };
            items.Add((CacheKey.Default($"key-{NodeInvariantIndexStrings.Format(i)}"), new NodeCacheEntry<object?> { Value = value, Version = 1 }));
        }

        return Task.FromResult(new SnapshotBenchmarkHost(dataDir, persistence, items));
    }

    internal Task<string> WriteNextSnapshotAsync()
    {
        _nextIndex++;
        return _writer.WriteAsync(_nextIndex, _items, [], CancellationToken.None);
    }
}
