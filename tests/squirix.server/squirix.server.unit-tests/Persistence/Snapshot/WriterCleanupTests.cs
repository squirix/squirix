using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Ensures failed snapshot writes do not leave stale temporary files.</summary>
[Immutable]
public sealed class WriterCleanupTests : IsolatedStorageTestBase
{
    /// <summary>Verifies a failed finalize leaves the previous final snapshot intact and removes the temporary file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedFinalizeKeepsPreviousSnapshot(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);
        var path = await writer.WriteSingleAsync(1, CacheKey.Default("stable"), BuildEntry("old"), cancellationToken);

        var failingWriter = new SnapshotWriter(Dir, new PublishFailingStorageFileOperations());
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException, string>(failingWriter.WriteSingleAsync(1, CacheKey.Default("replacement"), BuildEntry("new"), cancellationToken));

        _ = await Assert.That(File.Exists(path)).IsTrue();
        var singleKey = await Assert.That(await ReadSnapshotKeysAsync(path)).HasSingleItem();
        _ = await Assert.That(singleKey).IsEqualTo("stable");
        _ = await Assert.That(Directory.GetFiles(Dir, "*.tmp", SearchOption.TopDirectoryOnly)).IsEmpty();
    }

    /// <summary>Verifies a snapshot write failure removes the temporary file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedSerializationRemovesTmpFile(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);
        var items = FailingItems();
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, string>(writer.WriteAsync(1, items, [], cancellationToken));
        _ = await Assert.That(ex.Message).Contains("serialization", StringComparison.OrdinalIgnoreCase);
        _ = await Assert.That(Directory.GetFiles(Dir, "*.tmp", SearchOption.TopDirectoryOnly)).IsEmpty();
    }

    /// <summary>Verifies a snapshot writer replaces an existing final snapshot without leaving the path absent after success.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplaceNeedsNoExplicitPreDelete(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);
        var path = await writer.WriteSingleAsync(1, CacheKey.Default("stale"), BuildEntry("old"), cancellationToken);

        var rewrittenPath = await writer.WriteSingleAsync(1, CacheKey.Default("fresh"), BuildEntry("new"), cancellationToken);

        _ = await Assert.That(rewrittenPath).IsEqualTo(path);
        _ = await Assert.That(File.Exists(path)).IsTrue();
        var singleKey = await Assert.That(await ReadSnapshotKeysAsync(path)).HasSingleItem();
        _ = await Assert.That(singleKey).IsEqualTo("fresh");
        _ = await Assert.That(Directory.GetFiles(Dir, "*.tmp", SearchOption.TopDirectoryOnly)).IsEmpty();
    }

    /// <summary>Verifies a snapshot writer can create a new final snapshot file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteCreatesFinalWithoutPrecreate(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);

        var path = await writer.WriteSingleAsync(1, CacheKey.Default("a"), BuildEntry("first"), cancellationToken);

        _ = await Assert.That(File.Exists(path)).IsTrue();
        _ = await Assert.That(path).EndsWith(".bsqx", StringComparison.Ordinal);
        var singleKey = await Assert.That(await ReadSnapshotKeysAsync(path)).HasSingleItem();
        _ = await Assert.That(singleKey).IsEqualTo("a");
        _ = await Assert.That(Directory.GetFiles(Dir, "*.tmp", SearchOption.TopDirectoryOnly)).IsEmpty();
    }

    private static NodeCacheEntry<object?> BuildEntry(object? value) => new() { Value = value, Version = 1 };

    private static FailingAfterFirstItemList FailingItems() => new();

    private static async Task<List<string>> ReadSnapshotKeysAsync(string path)
    {
        var reader = StoreFactory.CreateReader();
        var loaded = await reader.LoadStrictAsync<object?>(path, cancellationToken: CancellationToken.None);
        var keys = new List<string>(loaded.Entries.Count);
        foreach (var (key, _) in loaded.Entries)
            keys.Add(key.Key);

        keys.Sort(StringComparer.Ordinal);
        return keys;
    }

    [Immutable]
    private sealed class FailingAfterFirstItemList : IReadOnlyList<(CacheKey Key, NodeCacheEntry<object?> Entry)>
    {
        public int Count => 2;

        public (CacheKey Key, NodeCacheEntry<object?> Entry) this[int index] =>
            index switch
            {
                0 => (new CacheKey("default", "a"), BuildEntry(1)),
                _ => throw new InvalidOperationException("simulated serialization failure"),
            };

        public IEnumerator<(CacheKey Key, NodeCacheEntry<object?> Entry)> GetEnumerator()
        {
            yield return this[0];
            _ = this[1];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [Immutable]
    private sealed class PublishFailingStorageFileOperations : IStorageFileOperations
    {
        private readonly FileOperations _inner = new();

        public bool PublishSnapshot(string tempPath, string finalPath) => throw new IOException("simulated snapshot publish failure");

        public bool TryDelete(string path) => _inner.TryDelete(path);
    }
}
