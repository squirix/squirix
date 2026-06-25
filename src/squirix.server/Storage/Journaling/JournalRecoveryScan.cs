using System;
using System.Buffers;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Platform;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Startup recovery and next-sequence determination for the journal pipeline. Extracted from
/// <see cref="JournalCoordinator" /> so the coordinator focuses on the live append/roll/group-commit
/// event loop (audit item A2).
/// </summary>
internal static class JournalRecoveryScan
{
    internal static ulong DetermineNextSequence(ManifestState manifest, PersistenceOptions options)
    {
        var next = manifest.NextSequence is 0UL ? 1UL : manifest.NextSequence;
        if (manifest.LastSnapshot?.LastAppliedSequence is { } lastApplied && lastApplied >= next)
            next = lastApplied + 1UL;

        var manifestCurrentJournal = manifest.CurrentJournal > 0 ? manifest.CurrentJournal : 1;
        var firstAvailableSegment = 0;
        var lastAvailableSegment = 0;
        foreach (var segment in JournalReadPath.EnumerateSegments(options.DataDir, 1))
        {
            if (firstAvailableSegment is 0)
                firstAvailableSegment = segment.Index;

            lastAvailableSegment = segment.Index;
        }

        ThrowIfJournalOnlyTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);
        var scanStartSegment = firstAvailableSegment is 0 ? 1 : Math.Max(firstAvailableSegment, manifestCurrentJournal);

        foreach (var record in JournalReadPath.ReadAll(options.DataDir, scanStartSegment, CancellationToken.None))
        {
            if (record.Sequence >= next)
                next = record.Sequence + 1UL;
        }

        return next;
    }

    internal static async Task PrepareActiveSegmentForSequenceScanAsync(ManifestState manifest, PersistenceOptions options, CancellationToken cancellationToken)
    {
        var segmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var path = JournalReadPath.BuildSegmentPath(options.DataDir, segmentIndex);
        if (!File.Exists(path))
            return;

        var writer = JournalSegmentWriterFactory.Create(options.JournalPlatformBackend);
        await using (writer.ConfigureAwait(false))
        {
            writer.OpenSegment(path, true);
            if (writer.Length == 0)
                return;

            await RepairTornTailIfNeededAsync(writer, path, cancellationToken).ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private static long ComputeValidLength(FileStream stream)
    {
        if (stream.Length == 0)
            return 0;

        stream.Position = 0;
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        if (!StreamEx.TryReadExact(stream, header))
            throw new InvalidDataException("journal segment has a truncated file header.");

        JournalFraming.EnsureSegmentHeaderSupported(header);

        long validLength = JournalFraming.FileHeaderSize;
        while (true)
        {
            var read = JournalFrameReader.ReadNext(stream, validLength, out var rentedBuffer, out _);
            if (read.Status is JournalFrameReadStatus.EndOfFile or not JournalFrameReadStatus.Success)
                return validLength;

            validLength = read.NextFrameOffset;
            if (rentedBuffer is not null)
                ArrayPool<byte>.Shared.Return(rentedBuffer);
        }
    }

    private static InvalidDataException CreateJournalTopologyDisjointForSequenceInit(int manifestCurrentJournal, int firstAvailableSegment, int lastAvailableSegment) => new(
        $"journal recovery cannot determine a valid replay start. manifestCurrentJournal={manifestCurrentJournal.ToString(CultureInfo.InvariantCulture)}, firstAvailableJournal={(firstAvailableSegment > 0 ? firstAvailableSegment : 0).ToString(CultureInfo.InvariantCulture)}, lastAvailableJournal={(lastAvailableSegment > 0 ? lastAvailableSegment : 0).ToString(CultureInfo.InvariantCulture)}, chosenReplayStartSegment=0, snapshotPresent=False.");

    private static async Task<long> ReadValidSegmentLengthAsync(string path, CancellationToken cancellationToken)
    {
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ComputeValidLength(stream);
        }
        finally
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task RepairTornTailIfNeededAsync(IJournalSegmentWriter writer, string path, CancellationToken cancellationToken)
    {
        try
        {
            var length = await ReadValidSegmentLengthAsync(path, cancellationToken).ConfigureAwait(false);
            if (length == writer.Length)
                return;

            writer.Truncate(length);
            if (length == 0)
                WriteFreshFileHeader(writer);

            writer.Fsync();
        }
        catch (InvalidDataException) when (writer.Length > 0)
        {
            writer.Truncate(0);
            WriteFreshFileHeader(writer);
            writer.Fsync();
        }
    }

    private static void ThrowIfJournalOnlyTopologyDisjointForSequenceInit(int manifestCurrentJournal, int firstAvailableSegment, int lastAvailableSegment)
    {
        if (firstAvailableSegment is 0)
        {
            if (manifestCurrentJournal is not 1)
                throw CreateJournalTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);

            return;
        }

        if (lastAvailableSegment < manifestCurrentJournal)
            throw CreateJournalTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);
    }

    private static void WriteFreshFileHeader(IJournalSegmentWriter writer)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        writer.Write(header, 0);
    }
}
