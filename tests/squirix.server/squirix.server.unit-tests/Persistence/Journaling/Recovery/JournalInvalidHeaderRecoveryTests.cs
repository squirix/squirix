using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>journal segment header validation during recovery and coordinator repair.</summary>
[Immutable]
public sealed class JournalInvalidHeaderRecoveryTests : ServerUnitTestBase
{
    private static readonly byte[] InvalidJournalHeaderBad = [0x42, 0x41, 0x44, 0x21, 0x21];
    private static readonly byte[] InvalidJournalHeaderNope = [0x4E, 0x4F, 0x50, 0x45, 0x21];

    /// <summary>Appending to a segment with an invalid header rewrites a valid file header before new frames.</summary>
    [Fact]
    public async Task CoordinatorWritesHeaderAfterInvalidSegmentRepair()
    {
        using var dir = new TempDirectory("squirix-journal-invalid-header-repair");
        var persistence = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        using var manifestStore = new Ledger(persistence);
        var journalSegmentPath = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, InvalidJournalHeaderBad, DefaultCancellationToken);
        await manifestStore.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = null }, DefaultCancellationToken);

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
        var journalSegmentPath = NodePathKit.Combine(scenario.DataDir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, InvalidJournalHeaderNope, DefaultCancellationToken);

        await scenario.Ledger.WriteAsync(
            new State
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
            new RecoveryDependencies<object?>(persistence, scenario.Ledger, scenario.Cache, gate, new RpcMutationIdempotencyStore(), StoreFactory.CreateReader(persistence)));

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(recovery.StartAsync(DefaultCancellationToken));

        Assert.Contains("invalid or missing journal file header", ex.Message, StringComparison.Ordinal);

        using var gateWait = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(gate.WaitAsync(gateWait.Token));
    }

    private static byte[] BuildPutPayload(string value) => JournalEntryPayloadKit.EncodePut(value);
}
