using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Verifies snapshot writer encode buffers stay correct across writes with varying record sizes.</summary>
[Immutable]
public sealed class WriterEncodeBufferTests : IsolatedStorageTestBase
{
    /// <summary>Verifies an empty snapshot write rents and releases its buffer without producing records.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptySnapshotRoundTripsNoRecords(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);

        var path = await writer.WriteAsync(1, [], [], cancellationToken);

        _ = await Assert.That(File.Exists(path)).IsTrue();
        var loaded = await LoadEntriesAsync(path);
        _ = await Assert.That(loaded).IsEmpty();
    }

    /// <summary>Verifies consecutive writes with record sizes above and below the historical initial buffer size round-trip.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VaryingRecordSizesRoundTripAsync(CancellationToken cancellationToken)
    {
        var writer = new SnapshotWriter(Dir);
        var largeValue = new string('x', 128 * 1024);

        var largePath = await writer.WriteSingleAsync(1, CacheKey.Default("large"), BuildEntry(largeValue), cancellationToken);
        var smallPath = await writer.WriteSingleAsync(2, CacheKey.Default("small"), BuildEntry("v"), cancellationToken);

        var largeEntries = await LoadEntriesAsync(largePath);
        var smallEntries = await LoadEntriesAsync(smallPath);
        _ = await Assert.That(largeEntries).HasSingleItem();
        _ = await Assert.That(smallEntries).HasSingleItem();
        _ = await Assert.That(largeEntries["large"]).IsEqualTo(largeValue);
        _ = await Assert.That(smallEntries["small"]).IsEqualTo("v");
    }

    private static NodeCacheEntry<object?> BuildEntry(object? value) => new() { Value = value, Version = 1 };

    private static async Task<Dictionary<string, object?>> LoadEntriesAsync(string path)
    {
        var reader = StoreFactory.CreateReader();
        var loaded = await reader.LoadStrictAsync<object?>(path, cancellationToken: CancellationToken.None);
        var entries = new Dictionary<string, object?>(loaded.Entries.Count, StringComparer.Ordinal);
        foreach (var (key, entry) in loaded.Entries)
            entries[key.Key] = entry.Value;

        return entries;
    }
}
