using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Entries;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>journal segment header validation during recovery and coordinator repair.</summary>
public sealed class JournalInvalidHeaderRecoveryTests : UnitTestBase
{
    /// <summary>Appending to a segment with an invalid header rewrites a valid file header before new frames.</summary>
    [Fact]
    public async Task CoordinatorWritesHeaderAfterInvalidSegmentRepair()
    {
        using var dir = new TempDirectory("squirix-journal-invalid-header-repair");
        var persistence = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        using var manifestStore = new ManifestStore(persistence);
        var journalSegmentPath = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, [.. "BAD!!"u8], DefaultCancellationToken);
        await manifestStore.WriteAsync(new Manifest { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = null }, DefaultCancellationToken);

        await using (var journal = await JournalCoordinatorFactory.CreateAsync(
                         persistence,
                         await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
                         manifestStore,
                         new JournalStartupGate(),
                         DefaultCancellationToken))
        {
            await journal.AppendPutAsync(CacheKey.Default("k"), await BuildEntryJsonAsync("v"), null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        }

        var bytes = await File.ReadAllBytesAsync(journalSegmentPath, DefaultCancellationToken);
        Assert.True(bytes.AsSpan(0, 4).SequenceEqual(JournalFraming.Magic));
        Assert.Equal(JournalFraming.Version, bytes[4]);
    }

    /// <summary>Recovery fails when a required journal segment has an invalid header; startup gate stays closed.</summary>
    [Fact]
    public async Task RecoveryFailsOnInvalidJournalHeader()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-journal-invalid-header-recovery");
        var journalSegmentPath = PathKit.Combine(scenario.DataDir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, [.. "NOPE!"u8], DefaultCancellationToken);

        await scenario.ManifestStore.WriteAsync(
            new Manifest
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 1,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        var gate = new JournalStartupGate(false);
        var recovery = new RecoveryService<object?>(
            new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 },
            scenario.ManifestStore,
            scenario.Cache,
            new RecoveryOptions { BlockOnStart = true },
            gate,
            NullLogger<RecoveryService<object?>>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(async () => { await recovery.StartAsync(DefaultCancellationToken); });

        Assert.Contains("invalid or missing journal file header", ex.Message, StringComparison.Ordinal);

        using var gateWait = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => { await gate.WaitAsync(gateWait.Token).AsTask(); });
    }

    private static Task<byte[]> BuildEntryJsonAsync(string value) => DiscriminatedEntryJsonWriter.BuildEntryJsonAsync(value, null, null, 1, null);
}
