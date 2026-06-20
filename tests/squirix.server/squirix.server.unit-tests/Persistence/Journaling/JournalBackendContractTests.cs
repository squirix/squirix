using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Contract tests for pluggable journal backends.</summary>
public sealed class JournalBackendContractTests
{
    /// <summary>Append and replay round-trip for each configured journal backend.</summary>
    /// <param name="backendOrdinal"><see cref="JournalBackend"/> ordinal under test.</param>
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public async Task AppendPutReplayRoundTrip(int backendOrdinal)
    {
        var backend = backendOrdinal switch
        {
            0 => JournalBackend.JsonFramed,
            1 => JournalBackend.Pipelined,
            _ => throw new ArgumentOutOfRangeException(nameof(backendOrdinal), backendOrdinal, null),
        };

        var dir = DirectoryKit.CreateTempDirectory("journal-contract");
        var options = new PersistenceOptions { DataDir = dir, JournalBackend = backend, JournalMaxSegmentMb = 64 };
        using var manifestStore = new ManifestStore(options);
        var gate = new JournalStartupGate();
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None);
        await using var coordinator = await JournalCoordinatorFactory.CreateAsync(options, manifest, manifestStore, gate, CancellationToken.None);

        var key = new CacheKey("ns", "k1");
        var payload = """{"v":1}"""u8.ToArray();
        await coordinator.AppendPutAsync(key, payload, "op-1", CancellationToken.None);
        await coordinator.AwaitDurabilityCommitAsync(CancellationToken.None);

        manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None);
        JournalRecord? last = null;
        foreach (var record in JournalReadPath.ReadAll(dir, manifest.CurrentJournal, CancellationToken.None))
        {
            last = record;
        }

        Assert.NotNull(last);
        Assert.Equal(JournalOperationKind.Put, last.Operation);
        Assert.Equal(key.Key, last.Key.Key);
    }
}
