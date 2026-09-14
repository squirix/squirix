using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Verifies snapshot writer encode buffers stay correct across writes with varying record sizes.</summary>
[Immutable]
public sealed class WriterEncodeBufferTests : IsolatedStorageTestBase
{
    /// <summary>Verifies an empty snapshot write rents and releases its buffer without producing records.</summary>
    [Fact]
    public async Task EmptySnapshotRoundTripsNoRecords()
    {
        var writer = new SnapshotWriter(Dir);

        var path = await writer.WriteAsync(1, [], [], DefaultCancellationToken);

        Assert.True(File.Exists(path));
        var loaded = await LoadEntriesAsync(path);
        Assert.Empty(loaded);
    }

    /// <summary>Verifies consecutive writes with record sizes above and below the historical initial buffer size round-trip.</summary>
    [Fact]
    public async Task VaryingRecordSizesRoundTripAsync()
    {
        var writer = new SnapshotWriter(Dir);
        var largeValue = new string('x', 128 * 1024);

        var largePath = await writer.WriteSingleAsync(1, CacheKey.Default("large"), BuildEntry(largeValue), DefaultCancellationToken);
        var smallPath = await writer.WriteSingleAsync(2, CacheKey.Default("small"), BuildEntry("v"), DefaultCancellationToken);

        var largeEntries = await LoadEntriesAsync(largePath);
        var smallEntries = await LoadEntriesAsync(smallPath);
        _ = Assert.Single(largeEntries);
        _ = Assert.Single(smallEntries);
        Assert.Equal(largeValue, largeEntries["large"]);
        Assert.Equal("v", smallEntries["small"]);
    }

    private static NodeCacheEntry<object?> BuildEntry(object? value) => new() { Value = value, Version = 1 };

    private static async Task<Dictionary<string, object?>> LoadEntriesAsync(string path)
    {
        var reader = StoreFactory.CreateReader(new PersistenceOptions { DataDir = Path.GetDirectoryName(path)! });
        var loaded = await reader.LoadStrictAsync<object?>(path, cancellationToken: CancellationToken.None);
        var entries = new Dictionary<string, object?>(loaded.Entries.Count, StringComparer.Ordinal);
        foreach (var (key, entry) in loaded.Entries)
            entries[key.Key] = entry.Value;

        return entries;
    }
}
