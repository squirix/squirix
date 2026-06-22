using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Ensures failed snapshot writes do not leave stale temporary files.</summary>
public sealed class SnapshotWriterCleanupTests : UnitTestBase
{
    /// <summary>Verifies a snapshot writer can create a new final snapshot file.</summary>
    [Fact]
    public async Task WriteAsyncCreatesNewSnapshotWhenFinalFileDoesNotExist()
    {
        using var dir = new TempDirectory("squirix-snap-writer-create");
        var writer = new SnapshotWriter(dir);

        var path = await writer.WriteAsync(1, [(CacheKey.Default("a"), BuildEntry("first"))], [], DefaultCancellationToken);

        Assert.True(File.Exists(path));
        Assert.EndsWith(".bsqx", path, StringComparison.Ordinal);
        Assert.Equal(["a"], await ReadSnapshotKeysAsync(path));
        Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
    }

    /// <summary>Verifies a failed finalize leaves the previous final snapshot intact and removes the temporary file.</summary>
    [Fact]
    public async Task WriteAsyncFailedFinalizeKeepsPreviousSnapshot()
    {
        using var dir = new TempDirectory("squirix-snap-writer-finalize-fail");
        var writer = new SnapshotWriter(dir);
        var path = await writer.WriteAsync(1, [(CacheKey.Default("stable"), BuildEntry("old"))], [], DefaultCancellationToken);

        var failingWriter = new SnapshotWriter(dir, new PublishFailingStorageFileOperations());
        _ = await Assert.ThrowsAnyAsync<IOException>(async () =>
        {
            _ = await failingWriter.WriteAsync(1, [(CacheKey.Default("replacement"), BuildEntry("new"))], [], DefaultCancellationToken);
        });

        Assert.True(File.Exists(path));
        Assert.Equal(["stable"], await ReadSnapshotKeysAsync(path));
        Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
    }

    /// <summary>Verifies a snapshot write failure removes the temporary file.</summary>
    [Fact]
    public async Task WriteAsyncRemovesTmpWhenSerializationFails()
    {
        using var dir = new TempDirectory("squirix-snap-writer-tmp");
        var writer = new SnapshotWriter(dir);
        var items = FailingItems();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => { _ = await writer.WriteAsync(1, items, [], DefaultCancellationToken); });
        Assert.Contains("serialization", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
    }

    /// <summary>Verifies a snapshot writer replaces an existing final snapshot without leaving the path absent after success.</summary>
    [Fact]
    public async Task WriteAsyncReplacesExistingSnapshotWithoutPreDelete()
    {
        using var dir = new TempDirectory("squirix-snap-writer-replace");
        var writer = new SnapshotWriter(dir);
        var path = await writer.WriteAsync(1, [(CacheKey.Default("stale"), BuildEntry("old"))], [], DefaultCancellationToken);

        var rewrittenPath = await writer.WriteAsync(1, [(CacheKey.Default("fresh"), BuildEntry("new"))], [], DefaultCancellationToken);

        Assert.Equal(path, rewrittenPath);
        Assert.True(File.Exists(path));
        Assert.Equal(["fresh"], await ReadSnapshotKeysAsync(path));
        Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
    }

    private static CacheEntry<object?> BuildEntry(object? value) => new() { Value = value, Version = 1 };

    private static IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> EnumerateThenFail()
    {
        yield return (new CacheKey("default", "a"), BuildEntry(1));
        throw new InvalidOperationException("simulated serialization failure");
    }

    /// <summary>Produces one valid entry and then fails during deferred enumeration to simulate a mid-stream serialization failure.</summary>
    private static IEnumerable<(CacheKey Key, CacheEntry<object?> Entry)> FailingItems() => EnumerateThenFail();

    private static async Task<string[]> ReadSnapshotKeysAsync(string path)
    {
        var reader = new SnapshotReader();
        var loaded = await reader.LoadStrictAsync<object?>(path, cancellationToken: CancellationToken.None);
        var keys = new List<string>(loaded.Entries.Count);
        foreach (var (key, _) in loaded.Entries)
            keys.Add(key.Key);

        keys.Sort(StringComparer.Ordinal);
        return keys.ToArray();
    }

    private sealed class PublishFailingStorageFileOperations : IStorageFileOperations
    {
        private readonly StorageFileOperations _inner = new();

        public bool PublishSnapshot(string tempPath, string finalPath) => throw new IOException("simulated snapshot publish failure");

        public bool TryDelete(string path) => _inner.TryDelete(path);
    }
}
