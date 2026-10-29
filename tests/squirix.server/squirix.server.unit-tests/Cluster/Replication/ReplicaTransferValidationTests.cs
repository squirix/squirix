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

/// <summary>Validation diagnostics explain checksum refusals without changing the refusal code or completeness rules.</summary>
[Immutable]
public sealed class ReplicaTransferValidationTests : ServerUnitTestBase
{
    private const string Transfer = "validation-transfer";

    /// <summary>Malformed declarations are diagnosed before any data is changed.</summary>
    /// <param name="reason">The invalid declaration and expected diagnostic.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("missing-transfer-id")]
    [Arguments("nonpositive-snapshot-length")]
    [Arguments("snapshot-length-exceeds-limit")]
    [Arguments("zero-index-term")]
    [Arguments("index-term-exceeds-leader-term")]
    public async Task BeginExplainsInvalidDeclaration(string reason, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("transfer-validation-begin");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var header = HeaderOf(string.Equals(reason, "missing-transfer-id", StringComparison.Ordinal) ? string.Empty : Transfer, 1UL);
        var baseline = BaselineOf(TransferFixtures.Image());
        baseline = reason switch
        {
            "nonpositive-snapshot-length" => baseline with { SnapshotBytes = 0 },
            "snapshot-length-exceeds-limit" => baseline with { SnapshotBytes = int.MaxValue },
            "zero-index-term" => baseline with { IndexTerm = 0UL },
            "index-term-exceeds-leader-term" => baseline with { IndexTerm = 2UL },
            _ => baseline,
        };

        var reply = await harness.Receiver.BeginAsync(header, baseline, cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        _ = await Assert.That(harness.Log.Count(4085, reason)).IsEqualTo(1);
        _ = await Assert.That(harness.Sink.Calls.Count).IsEqualTo(0);
    }

    /// <summary>Corrupt image headers and CRCs are distinguished while the old copy remains complete.</summary>
    /// <param name="offset">The byte to corrupt in the published image; minus one selects the trailing CRC.</param>
    /// <param name="reason">The expected diagnostic.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0, "snapshot-magic-mismatch")]
    [Arguments(4, "snapshot-version-unsupported")]
    [Arguments(5, "snapshot-payload-length-mismatch")]
    [Arguments(-1, "snapshot-payload-crc-mismatch")]
    public async Task SnapshotExplainsImageFailure(int offset, string reason, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("transfer-validation-image");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var image = TransferFixtures.Image();
        var header = HeaderOf(Transfer, 1UL);
        _ = await harness.Receiver.BeginAsync(header, BaselineOf(image), cancellationToken);
        image[offset < 0 ? image.Length - 1 : offset] ^= 1;

        var reply = await harness.Receiver.SnapshotAsync(in header, 1, 0, image, cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        _ = await Assert.That(harness.Log.Count(4085, reason)).IsEqualTo(1);
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log) && log.IsComplete).IsTrue();
        _ = await Assert.That(harness.Sink.Calls.Count).IsEqualTo(2);
    }

    /// <summary>Valid snapshot images are checked against the separately declared baseline.</summary>
    /// <param name="reason">The baseline field that disagrees with the image.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="ArgumentOutOfRangeException">The diagnostic case is unknown.</exception>
    [Test]
    [Arguments("snapshot-checksum-declaration-mismatch")]
    [Arguments("snapshot-index-mismatch")]
    [Arguments("snapshot-index-term-mismatch")]
    public async Task SnapshotExplainsDeclarationMismatch(string reason, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("transfer-validation-baseline");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var image = TransferFixtures.Image();
        var baseline = BaselineOf(image);
        baseline = reason switch
        {
            "snapshot-checksum-declaration-mismatch" => baseline with { SnapshotChecksum = baseline.SnapshotChecksum ^ 1U },
            "snapshot-index-mismatch" => baseline with { Index = baseline.Index + 1UL },
            "snapshot-index-term-mismatch" => baseline with { IndexTerm = 2UL },
            _ => throw new ArgumentOutOfRangeException(nameof(reason)),
        };
        var header = HeaderOf(Transfer, 3UL);
        _ = await harness.Receiver.BeginAsync(header, baseline, cancellationToken);

        var reply = await harness.Receiver.SnapshotAsync(in header, 1, 0, image, cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        _ = await Assert.That(harness.Log.Count(4085, reason)).IsEqualTo(1);
    }

    /// <summary>Final count and CRC mismatches log both totals and transfer identity, and never mark the copy complete.</summary>
    /// <param name="wrongCount">Whether the count disagrees; otherwise the CRC disagrees.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EndReportsBothTotals(bool wrongCount, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("transfer-validation-totals");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        ReplicaStateEntry[] entries = [Entry("key", 7)];
        sequence = await harness.SendBatchAsync(Transfer, sequence, entries, cancellationToken);
        var actual = RecordingStateSink.TotalsOf(entries);
        var expected = wrongCount ? actual with { EntryCount = actual.EntryCount + 1UL } : actual with { Crc = actual.Crc ^ 1U };

        var reply = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, expected.EntryCount, expected.Crc, cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        var message = harness.Log.FindMessage(4086)!;
        _ = await Assert.That(message.Contains($"expected {expected}, actual {actual}", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains(Transfer, StringComparison.Ordinal) && message.Contains(GroupId, StringComparison.Ordinal) && message.Contains(LeaderId, StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(message.Contains("Node receiver", StringComparison.Ordinal) && message.Contains("at End", StringComparison.Ordinal)).IsTrue();
        _ = await Assert.That(harness.Sink.ResetIndex).IsNull();
        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log) && !log.IsComplete).IsTrue();
    }
}
