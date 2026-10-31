using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
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

/// <summary>Verifies the node snapshot file is not bounded by a 32-bit length.</summary>
[Immutable]
public sealed class WriterMetricsTests : IsolatedStorageTestBase
{
    private const int EntryCountPastTwoGiB = 520;

    private const int SharedValueBytes = 4 * 1024 * 1024;

    /// <summary>A snapshot whose encoded size passes the 32-bit range reports its size instead of failing.</summary>
    [Test]
    public async Task MetricsReportSizePastTwoGiB()
    {
        var items = BuildItems(EntryCountPastTwoGiB, new byte[SharedValueBytes]);

        var (totalFileSize, maxRecordLength) = SnapshotFileEncoder.ComputeWriteMetrics(items, [], new int[items.Count]);

        _ = await Assert.That(totalFileSize).IsGreaterThan(int.MaxValue);
        _ = await Assert.That(maxRecordLength).IsGreaterThan(SharedValueBytes);
        _ = await Assert.That(maxRecordLength).IsLessThan(SharedValueBytes + 1024);
    }

    /// <summary>A snapshot file past 2 GiB is written and loaded back with every entry and a valid footer checksum; it takes minutes and several GiB of disk and memory, so it runs only when selected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Explicit]
    public async Task FilePastTwoGiBRoundTrips(CancellationToken cancellationToken)
    {
        var value = new byte[SharedValueBytes];
        RandomNumberGenerator.Fill(value);
        var items = BuildItems(EntryCountPastTwoGiB, value);
        var path = Path.Join(Dir, "large-snapshot");
        Directory.CreateDirectory(path);
        try
        {
            var writer = new SnapshotWriter(path);

            var snapshotPath = await writer.WriteAsync(1, items, [], cancellationToken);

            _ = await Assert.That(new FileInfo(snapshotPath).Length).IsGreaterThan(int.MaxValue);
            var loaded = await StoreFactory.CreateReader().LoadStrictAsync<object?>(snapshotPath, null, cancellationToken);
            _ = await Assert.That(loaded.Entries.Count).IsEqualTo(EntryCountPastTwoGiB);
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var (key, entry) in loaded.Entries)
            {
                _ = seen.Add(key.Key);
                _ = await Assert.That(entry.Value is byte[] bytes && bytes.AsSpan().SequenceEqual(value)).IsTrue();
            }

            _ = await Assert.That(seen.Count).IsEqualTo(EntryCountPastTwoGiB);
            _ = await Assert.That(seen.Contains(KeyName(0))).IsTrue();
            _ = await Assert.That(seen.Contains(KeyName(EntryCountPastTwoGiB - 1))).IsTrue();
        }
        finally
        {
            Directory.Delete(path, true);
        }
    }

    private static List<(CacheKey Key, NodeCacheEntry<object?> Entry)> BuildItems(int count, byte[] sharedValue)
    {
        var items = new List<(CacheKey Key, NodeCacheEntry<object?> Entry)>(count);
        for (var i = 0; i < count; i++)
            items.Add((CacheKey.Default(KeyName(i)), new NodeCacheEntry<object?> { Value = sharedValue, Version = 1 }));

        return items;
    }

    private static string KeyName(int index) => $"k{index}";
}
