using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
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
        var journalSegmentPath = PathKit.Combine(dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, [.. "BAD!!"u8], DefaultCancellationToken);
        await manifestStore.WriteAsync(new Storage.Manifest.State { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = null }, DefaultCancellationToken);

        await using (var journal = await JournalCoordinatorFactory.CreateAsync(
                         persistence,
                         await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
                         manifestStore,
                         new JournalStartupGate(),
                         DefaultCancellationToken))
        {
            await journal.AppendPutAsync(CacheKey.Default("k"), BuildPutPayload("v"), DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
        }

        var bytes = await File.ReadAllBytesAsync(journalSegmentPath, DefaultCancellationToken);
        Assert.True(bytes.AsSpan(0, 4).SequenceEqual("SJRN"u8));
        Assert.Equal(JournalFraming.Version, bytes[4]);
    }

    /// <summary>Recovery fails when a required journal segment has an invalid header; startup gate stays closed.</summary>
    [Fact]
    public async Task RecoveryFailsOnInvalidJournalHeader()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-journal-invalid-header-recovery");
        var journalSegmentPath = PathKit.Combine(scenario.DataDir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, [.. "NOPE!"u8], DefaultCancellationToken);

        await scenario.ManifestStore.WriteAsync(
            new Storage.Manifest.State
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 1,
                LastSnapshot = null,
            },
            DefaultCancellationToken);

        var gate = new JournalStartupGate(false);
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                scenario.ManifestStore,
                scenario.Cache,
                gate,
                new RpcMutationIdempotencyStore(),
                StoreFactory.CreateReader(persistence)));

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() => recovery.StartAsync(DefaultCancellationToken));

        Assert.Contains("invalid or missing journal file header", ex.Message, StringComparison.Ordinal);

        using var gateWait = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await gate.WaitAsync(gateWait.Token).AsTask());
    }

    private static byte[] BuildPutPayload(string value) => JournalEntryPayloadKit.EncodePut(value);
}
