using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>journal segment header validation during recovery and coordinator repair.</summary>
[Immutable]
public sealed class JournalInvalidHeaderRecoveryTests : DisposableServerUnitTestBase
{
    private static readonly byte[] InvalidJournalHeaderBad = [0x42, 0x41, 0x44, 0x21, 0x21];
    private static readonly byte[] InvalidJournalHeaderNope = [0x4E, 0x4F, 0x50, 0x45, 0x21];

    private readonly Meter _testMeter = new("test");

    /// <summary>Appending to a segment with an invalid header rewrites a valid file header before new frames.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeaderRewrittenAfterSegmentRepair(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-journal-invalid-header-repair");
        var persistence = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        using var manifestStore = new Ledger(persistence);
        var journalSegmentPath = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, InvalidJournalHeaderBad, cancellationToken);
        await manifestStore.WriteAsync(new State { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = null }, cancellationToken);

        await using (var journal = JournalCoordinatorFactory.Create(
                         persistence,
                         await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
                         manifestStore,
                         new AsyncManualResetEvent(true)))
        {
            await journal.AppendPutUnderGateAsync(CacheKey.Default("k"), BuildPutPayload("v"), cancellationToken);
            await journal.AwaitDurabilityCommitAsync(cancellationToken);
        }

        var bytes = await File.ReadAllBytesAsync(journalSegmentPath, cancellationToken);
        _ = await Assert.That(bytes.AsSpan(0, 4).SequenceEqual("SJRN"u8)).IsTrue();
        _ = await Assert.That(bytes[4]).IsEqualTo(JournalFraming.Version);
    }

    /// <summary>Recovery fails when a required journal segment has an invalid header; startup gate stays closed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveryFailsOnInvalidJournalHeader(CancellationToken cancellationToken)
    {
        using var scenario = RecoveryScenarioBuilder.Create("squirix-journal-invalid-header-recovery");
        var journalSegmentPath = NodePathKit.Combine(scenario.DataDir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, InvalidJournalHeaderNope, cancellationToken);

        await scenario.Ledger.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 1,
                LastSnapshot = null,
            },
            cancellationToken);

        var gate = new AsyncManualResetEvent();
        var persistence = new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
        var recovery = new RecoveryService<object?>(
            new RecoveryOptions { BlockOnStart = true },
            NullLogger<RecoveryService<object?>>.Instance,
            new RecoveryDependencies<object?>(
                persistence,
                scenario.Ledger,
                scenario.Cache,
                gate,
                new RpcMutationIdempotencyStore(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter)),
                StoreFactory.CreateReader()));

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(recovery.StartAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("invalid or missing journal file header", StringComparison.Ordinal);

        using var gateWait = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(gate.WaitAsync(gateWait.Token));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static byte[] BuildPutPayload(string value) => JournalEntryPayloadKit.EncodePut(value);
}
