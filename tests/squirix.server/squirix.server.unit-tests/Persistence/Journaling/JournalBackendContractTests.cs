using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Contract tests for the pipelined journal coordinator.</summary>
public sealed class JournalBackendContractTests
{
    /// <summary>Append and replay round-trip for the pipelined journal backend.</summary>
    [Fact]
    public async Task AppendPutReplayRoundTrip()
    {
        using var dir = new TempDirectory("journal-contract");
        var options = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 64 };
        using var manifestStore = new ManifestStore(options);
        var gate = new JournalStartupGate();
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None);
        await using var coordinator = await JournalCoordinatorFactory.CreateAsync(options, manifest, manifestStore, gate, CancellationToken.None);

        var key = new CacheKey("ns", "k1");
        var payload = JournalEntryPayloadKit.EncodePut(1);
        await coordinator.AppendPutAsync(key, payload, "op-1", CancellationToken.None);
        await coordinator.AwaitDurabilityCommitAsync(CancellationToken.None);

        manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None);
        JournalRecord? last = null;
        foreach (var record in JournalReadPath.ReadAll(dir, manifest.CurrentJournal, CancellationToken.None))
            last = record;

        Assert.NotNull(last);
        Assert.Equal(JournalOperationKind.Put, last.Operation);
        Assert.Equal(key.Key, last.Key.Key);
    }
}
