using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// journal segment header validation during recovery and writer repair.
/// </summary>
public sealed class JournalInvalidHeaderRecoveryTests : ServerUnitTestBase
{
    /// <summary>
    /// Recovery fails when a required journal segment has an invalid header; startup gate stays closed.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task RecoveryFailsOnInvalidJournalHeader()
    {
        await using var scenario = RecoveryScenarioBuilder.Create("squirix-journal-invalid-header-recovery");
        var journalSegmentPath = PathKit.Combine(scenario.DataDir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await File.WriteAllBytesAsync(journalSegmentPath, [.. "NOPE!"u8], DefaultCancellationToken);

        scenario.ManifestStore.Write(
            new Manifest
            {
                Format = 1,
                CurrentJournal = 1,
                NextSequence = 1,
                LastSnapshot = null,
            });

        var gate = new JournalStartupGate(false);
        var recovery = new RecoveryService<object?>(
            new PersistenceOptions { DataDir = scenario.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 },
            scenario.ManifestStore,
            scenario.Cache,
            new RecoveryOptions { BlockOnStart = true },
            gate,
            NullLogger<RecoveryService<object?>>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() => recovery.StartAsync(DefaultCancellationToken));

        Assert.Contains("invalid or missing journal file header", ex.Message, StringComparison.Ordinal);

        using var gateWait = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => gate.WaitAsync(gateWait.Token).AsTask());
    }

    /// <summary>
    /// Appending to a segment with an invalid header rewrites a valid file header before new frames.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task JournalWriterWritesHeaderAfterInvalidSegmentRepair()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-invalid-header-repair");
        try
        {
            var persistence = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
            var manifestStore = new ManifestStore(persistence);
            var journalSegmentPath = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
            await File.WriteAllBytesAsync(journalSegmentPath, [.. "BAD!!"u8], DefaultCancellationToken);
            manifestStore.Write(new Manifest { Format = 1, CurrentJournal = 1, NextSequence = 1, LastSnapshot = null });

            await using (var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate()))
            {
                await journal.AppendPutAsync(CacheKey.Default("k"), BuildEntryJson("v"), null, DefaultCancellationToken);
                await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
            }

            var bytes = await File.ReadAllBytesAsync(journalSegmentPath, DefaultCancellationToken);
            Assert.True(bytes.AsSpan(0, 4).SequenceEqual(JournalFraming.Magic));
            Assert.Equal(JournalFraming.Version, bytes[4]);

            await using var stream = File.OpenRead(journalSegmentPath);
            Assert.True(JournalSegmentFileVerifier.TryVerify(stream, DefaultCancellationToken, out var frames, out _, out var error), error);
            Assert.Equal(1, frames);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    private static byte[] BuildEntryJson(string value) => RecordCodec.Serialize(
        new JournalEnvelope
        {
            Seq = 1,
            UnixMs = 1,
            Put = new Put
            {
                Item = new EntryPair
                {
                    Key = "k",
                    EntryJson = ByteString.CopyFrom(Encoding.UTF8.GetBytes($"{{\"v\":{{\"$t\":\"s\",\"v\":\"{value}\"}},\"ver\":1}}")),
                },
            },
        });
}
