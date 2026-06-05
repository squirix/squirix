using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Ensures failed snapshot writes do not leave stale temporary files.
/// </summary>
public sealed class SnapshotWriterCleanupTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies a snapshot writer can create a new final snapshot file.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task WriteAsyncCreatesNewSnapshotWhenFinalFileDoesNotExist()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-snap-writer-create");
        try
        {
            var writer = new SnapshotWriter(dir);

            var path = await writer.WriteAsync(1, [(CacheKey.Default("a"), BuildEntryJsonElement("first"))], DefaultCancellationToken);

            Assert.True(File.Exists(path));
            Assert.Equal(["a"], await ReadSnapshotKeysAsync(path));
            Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Verifies a failed finalize leaves the previous final snapshot intact and removes the temporary file.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task WriteAsyncFailedFinalizeKeepsPreviousSnapshot()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-snap-writer-finalize-fail");
        try
        {
            var writer = new SnapshotWriter(dir);
            var path = await writer.WriteAsync(1, [(CacheKey.Default("stable"), BuildEntryJsonElement("old"))], DefaultCancellationToken);

            var failingWriter = new SnapshotWriter(dir, new PublishFailingStorageFileOperations());
            _ = await Assert.ThrowsAnyAsync<IOException>(() => failingWriter.WriteAsync(
                1,
                [(CacheKey.Default("replacement"), BuildEntryJsonElement("new"))],
                DefaultCancellationToken));

            Assert.True(File.Exists(path));
            Assert.Equal(["stable"], await ReadSnapshotKeysAsync(path));
            Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Verifies a snapshot write failure removes the temporary file.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task WriteAsyncRemovesTmpWhenSerializationFails()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-snap-writer-tmp");
        try
        {
            var writer = new SnapshotWriter(dir);
            var items = FailingItems();
            _ = await Assert.ThrowsAsync<InvalidOperationException>(() => writer.WriteAsync(1, items, [], DefaultCancellationToken));
            Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Verifies a snapshot writer replaces an existing final snapshot without leaving the path absent after success.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task WriteAsyncReplacesExistingSnapshotWithoutPreDelete()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-snap-writer-replace");
        try
        {
            var writer = new SnapshotWriter(dir);
            var path = await writer.WriteAsync(1, [(CacheKey.Default("stale"), BuildEntryJsonElement("old"))], DefaultCancellationToken);

            var rewrittenPath = await writer.WriteAsync(1, [(CacheKey.Default("fresh"), BuildEntryJsonElement("new"))], DefaultCancellationToken);

            Assert.Equal(path, rewrittenPath);
            Assert.True(File.Exists(path));
            Assert.Equal(["fresh"], await ReadSnapshotKeysAsync(path));
            Assert.Empty(Directory.GetFiles(dir, "*.tmp", SearchOption.TopDirectoryOnly));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    private static JsonElement BuildEntryJsonElement(object? value)
    {
        var bytes = DiscriminatedEntryJsonWriter.BuildEntryJson(value, null, null, 1, null);
        using var doc = JsonDocument.Parse(bytes);
        return doc.RootElement.Clone();
    }

    private static IEnumerable<(CacheKey Key, object Entry)> FailingItems()
    {
        yield return (new CacheKey("default", "a"), 1);
        throw new InvalidOperationException("simulated serialization failure");
    }

    private static async Task<string[]> ReadSnapshotKeysAsync(string path)
    {
        var keys = new List<string>();
        await foreach (var (key, _) in SnapshotReader.ReadEntriesAsync<object?>(path, cancellationToken: CancellationToken.None))
            keys.Add(key.Key);

        return [.. keys.OrderBy(static x => x, StringComparer.Ordinal)];
    }

    private sealed class PublishFailingStorageFileOperations : IStorageFileOperations
    {
        private readonly StorageFileOperations _inner = new();

        public void PublishSnapshot(string tempPath, string finalPath) => throw new IOException("simulated snapshot publish failure");

        public bool TryDelete(string path) => _inner.TryDelete(path);
    }
}
