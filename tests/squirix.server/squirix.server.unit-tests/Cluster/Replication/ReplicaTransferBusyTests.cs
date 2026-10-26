using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaTransferHarness;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A receiver whose node journal is at its high-water mark refuses a step as busy, keeps the transfer and takes the same message again.</summary>
[Immutable]
public sealed class ReplicaTransferBusyTests : ServerUnitTestBase
{
    private const string Transfer = "t";

    /// <summary>A busy wipe step keeps the ticket and the expected sequence: the identical step is accepted once the journal has room.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusyWipeKeepsTicketAndSequence(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-busy-wipe");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        harness.Sink.BusyWipes = 2;

        var first = await harness.Receiver.WipeAsync(harness.HeaderFor(Transfer), sequence, cancellationToken);
        var second = await harness.Receiver.WipeAsync(harness.HeaderFor(Transfer), sequence, cancellationToken);
        var third = await harness.Receiver.WipeAsync(harness.HeaderFor(Transfer), sequence, cancellationToken);
        var next = await harness.SendBatchAsync(Transfer, sequence + 1, [Entry("k-1", 1)], cancellationToken);

        _ = await Assert.That((first.RefusalCode, second.RefusalCode)).IsEqualTo((RefusalCodes.JournalBusy, RefusalCodes.JournalBusy));
        _ = await Assert.That((third.Success, third.More)).IsEqualTo((true, false));
        _ = await Assert.That(next).IsEqualTo(sequence + 2);
        _ = await Assert.That(harness.Sink.Held).IsTrue();
    }

    /// <summary>A batch the journal took in part is refused as busy, and the identical batch sent again continues from the first entry not written.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResentBatchContinuesAfterWritten(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-busy-resume");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        var batch = new[] { Entry("k-1", 1), Entry("k-2", 2), Entry("k-3", 3) };
        harness.Sink.NextWriteLimit = 2;

        var partial = await harness.Receiver.EntriesAsync(harness.HeaderFor(Transfer), sequence, batch, cancellationToken);
        var resent = await harness.Receiver.EntriesAsync(harness.HeaderFor(Transfer), sequence, batch, cancellationToken);
        var totals = RecordingStateSink.TotalsOf(batch);
        var end = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence + 1, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That(partial.RefusalCode).IsEqualTo(RefusalCodes.JournalBusy);
        _ = await Assert.That(resent.Success).IsTrue();
        _ = await Assert.That((harness.Sink.WriteLengths.Count, harness.Sink.WriteLengths[0], harness.Sink.WriteLengths[1])).IsEqualTo((2, 3, 1));
        _ = await Assert.That(end.Success).IsTrue().Because("Every entry was counted and checksummed exactly once.");
    }

    /// <summary>A resent batch shorter than what was already written cannot be the same batch: the transfer ends as a checksum mismatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShortResentBatchFailsChecksum(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-busy-short");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        var batch = new[] { Entry("k-1", 1), Entry("k-2", 2), Entry("k-3", 3) };
        harness.Sink.NextWriteLimit = 2;
        _ = await harness.Receiver.EntriesAsync(harness.HeaderFor(Transfer), sequence, batch, cancellationToken);

        var shortened = await harness.Receiver.EntriesAsync(harness.HeaderFor(Transfer), sequence, batch.AsMemory(0, 1), cancellationToken);

        _ = await Assert.That((shortened.Success, shortened.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        _ = await Assert.That(harness.Sink.Held).IsFalse();
    }

    /// <summary>A begin at the high-water mark is refused as busy before the sink is entered: nothing is wiped and the copy stays complete.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BeginRefusedBusyKeepsCopyComplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-busy-begin");
        await using var harness = await OpenAsync(dir, cancellationToken);
        harness.Sink.IsJournalBusy = true;

        var busy = await harness.Receiver.BeginAsync(HeaderOf(Transfer, 1UL), BaselineOf(TransferFixtures.Image()), cancellationToken);
        harness.Sink.IsSnapshotFailed = true;
        var full = await harness.Receiver.BeginAsync(HeaderOf(Transfer, 1UL), BaselineOf(TransferFixtures.Image()), cancellationToken);

        _ = await Assert.That((busy.Success, busy.RefusalCode)).IsEqualTo((false, RefusalCodes.JournalBusy));
        _ = await Assert.That((full.Success, full.RefusalCode)).IsEqualTo((false, RefusalCodes.JournalFull));
        _ = await Assert.That(harness.Sink.Calls).IsEmpty();
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That(log!.IsComplete).IsTrue();
    }

    /// <summary>A busy step answered while the snapshot service failed for good ends the transfer as journal-full and releases the group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusyStepWithFailedSnapshotsEndsTransfer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-busy-full");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        harness.Sink.BusyWipes = 1;
        harness.Sink.IsSnapshotFailed = true;

        var reply = await harness.Receiver.WipeAsync(harness.HeaderFor(Transfer), sequence, cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.JournalFull));
        _ = await Assert.That(harness.Sink.Held).IsFalse();
    }

    /// <summary>A leader that stops resending a busy step is dropped by the idle timeout like any silent leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbandonedBusyTransferExpires(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-busy-idle");
        await using var harness = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { IdleTimeout = TimeSpan.FromSeconds(30) });
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        harness.Sink.BusyWipes = 1;
        harness.Clock.Advance(TimeSpan.FromSeconds(20));
        _ = await harness.Receiver.WipeAsync(harness.HeaderFor(Transfer), sequence, cancellationToken);
        harness.Clock.Advance(TimeSpan.FromSeconds(20));
        _ = await Assert.That(harness.Sink.Held).IsTrue().Because("The busy answer refreshed the activity.");

        harness.Clock.Advance(TimeSpan.FromSeconds(11));

        _ = await Assert.That(harness.Sink.Held).IsFalse();
    }
}
