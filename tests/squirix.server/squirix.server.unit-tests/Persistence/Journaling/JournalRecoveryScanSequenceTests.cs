using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies the next-sequence scan over journal segments.</summary>
[Immutable]
public sealed class JournalRecoveryScanSequenceTests : IsolatedStorageTestBase
{
    private const byte UnassignedOpcode = 8;

    /// <summary>A directory without segments yields the manifest baseline.</summary>
    /// <returns>A task that completes when the assertion finishes.</returns>
    [Test]
    public async Task EmptyJournalYieldsBaseline()
    {
        var next = JournalRecoveryScan.DetermineNextSequence(NewManifest(1, 7), NewPersistence());

        _ = await Assert.That(next).IsEqualTo(7UL);
    }

    /// <summary>The largest sequence across several segments from the manifest journal on decides the result.</summary>
    /// <returns>A task that completes when the assertion finishes.</returns>
    [Test]
    public async Task SeveralSegmentsYieldHighestSequence()
    {
        WriteSegment(1, 40UL, 41UL);
        WriteSegment(2, 5UL, 6UL, 7UL);
        WriteSegment(3, 9UL, 8UL);

        var next = JournalRecoveryScan.DetermineNextSequence(NewManifest(2, 1), NewPersistence());

        _ = await Assert.That(next).IsEqualTo(10UL);
    }

    /// <summary>A torn tail of the last segment is ignored.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the assertion finishes.</returns>
    [Test]
    public async Task TornTailIsIgnored(CancellationToken cancellationToken)
    {
        WriteSegment(1, 3UL, 4UL);
        var path = BinaryJournalTestSegmentWriter.SegmentPath(Dir, 1);
        byte[] tornFrame = [10, 0, 0, 0, 1, 2];
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        Array.Resize(ref bytes, bytes.Length + tornFrame.Length);
        tornFrame.CopyTo(bytes, bytes.Length - tornFrame.Length);
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);

        var next = JournalRecoveryScan.DetermineNextSequence(NewManifest(1, 1), NewPersistence());

        _ = await Assert.That(next).IsEqualTo(5UL);
    }

    /// <summary>A checksum mismatch fails the scan.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the assertion finishes.</returns>
    [Test]
    public async Task ChecksumMismatchFailsScan(CancellationToken cancellationToken)
    {
        WriteSegment(1, 3UL, 4UL);
        var path = BinaryJournalTestSegmentWriter.SegmentPath(Dir, 1);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (Manifest: NewManifest(1, 1), Persistence: NewPersistence()),
            static state => _ = JournalRecoveryScan.DetermineNextSequence(state.Manifest, state.Persistence));

        _ = await Assert.That(failure.Message).Contains("journal segment corruption", StringComparison.Ordinal);
    }

    /// <summary>A frame with an unassigned opcode fails the scan with the segment read failure.</summary>
    /// <returns>A task that completes when the assertion finishes.</returns>
    [Test]
    public async Task UnassignedOpcodeFailsScan()
    {
        BinaryJournalTestSegmentWriter.WriteRawOpcodeSegment(Dir, 1, UnassignedOpcode, "a", [BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "b", "v")]);

        var failure = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (Manifest: NewManifest(1, 1), Persistence: NewPersistence()),
            static state => _ = JournalRecoveryScan.DetermineNextSequence(state.Manifest, state.Persistence));

        _ = await Assert.That(failure.Message).Contains("failed reading journal segment", StringComparison.Ordinal);
    }

    private static State NewManifest(int currentJournal, ulong nextSequence) => new() { CurrentJournal = currentJournal, NextSequence = nextSequence };

    private PersistenceOptions NewPersistence() => new() { DataDir = Dir, JournalMaxSegmentMb = 16 };

    private void WriteSegment(int index, params ulong[] sequences)
    {
        var records = new JournalRecord[sequences.Length];
        for (var i = 0; i < sequences.Length; i++)
            records[i] = BinaryJournalTestSegmentWriter.BuildPutRecord(sequences[i], "k" + i.ToString(CultureInfo.InvariantCulture), "v");

        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, index, records);
    }
}
