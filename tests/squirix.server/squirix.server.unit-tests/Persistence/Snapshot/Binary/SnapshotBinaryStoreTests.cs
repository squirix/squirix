using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot.Binary;

/// <summary>Binary snapshot writer/reader integration tests.</summary>
public sealed class SnapshotBinaryStoreTests : UnitTestBase
{
    /// <summary>Writes mixed entries and idempotency records, then loads them back.</summary>
    [Fact]
    public async Task WriteAndReadRoundTripMixedEntries()
    {
        using var dir = new TempDirectory("squirix-binary-snapshot");
        var options = new PersistenceOptions { DataDir = dir.Path, SnapshotBackend = SnapshotBackend.Binary };
        var writer = SnapshotStoreFactory.CreateWriter(options);
        var reader = SnapshotStoreFactory.CreateReader(options);

        var items = new List<(CacheKey Key, CacheEntry<object?> Entry)>
        {
            (CacheKey.Default("alpha"), new CacheEntry<object?> { Value = "one", Version = 1 }),
            (CacheKey.Default("beta"), new CacheEntry<object?> { Value = 42L, Version = 2 }),
        };
        var idempotency = new[]
        {
            new PersistedIdempotencyRecord
            {
                OperationId = "op-1",
                Fingerprint = "fp-1",
                CreatedUtc = new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
                Outcome = new PersistedIdempotencyOutcome { Kind = "insert" },
            },
        };

        var path = await writer.WriteAsync(1, items, idempotency, DefaultCancellationToken);
        Assert.EndsWith(".bsqx", path, StringComparison.Ordinal);
        Assert.True(File.Exists(path));

        var loaded = await reader.LoadStrictAsync<object?>(path, cancellationToken: DefaultCancellationToken);
        Assert.Equal(2, loaded.Entries.Count);
        _ = Assert.Single(loaded.IdempotencyRecords);
        Assert.Equal("one", loaded.Entries[0].Entry.Value);
        Assert.Equal(42L, Assert.IsType<long>(loaded.Entries[1].Entry.Value));
    }
}
