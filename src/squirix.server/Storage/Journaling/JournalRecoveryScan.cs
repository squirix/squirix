using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Storage.Journaling.Abstractions;
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
    private enum HeaderState
    {
        Valid = 0,
        Damaged = 1,
        TornCreation = 2,
    }

    internal static ulong DetermineNextSequence(State manifest, PersistenceOptions options)
    {
        var next = ResolveBaselineNextSequence(manifest);
        var (firstAvailableSegment, lastAvailableSegment) = ProbeAvailableSegments(options.DataDir);
        var manifestCurrentJournal = manifest.CurrentJournal > 0 ? manifest.CurrentJournal : 1;
        ThrowIfJournalOnlyTopologyDisjoint(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);

        var scanStartSegment = firstAvailableSegment == 0 ? 1 : Math.Max(firstAvailableSegment, manifestCurrentJournal);
        using var records = JournalReadPath.ReadAll(options.DataDir, scanStartSegment, CancellationToken.None);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Sequence >= next)
                next = record.Sequence + 1UL;
        }

        return next;
    }

    /// <summary>
    /// Repairs the active segment and the pre-created roll-target segment before the sequence scan. Startup may cut only a torn tail,
    /// that is a suffix that provably holds no valid data; every other damage fails startup and leaves the file untouched.
    /// </summary>
    /// <param name="manifest">The current manifest.</param>
    /// <param name="options">Persistence options locating the segments.</param>
    /// <returns>Every repair that changed a segment file; empty when nothing changed.</returns>
    /// <exception cref="InvalidDataException">Thrown when a segment is damaged in a way that is not a torn tail or a torn creation.</exception>
    internal static IReadOnlyList<JournalRepair> PrepareActiveSegmentForSequenceScan(State manifest, PersistenceOptions options)
    {
        List<JournalRepair>? repairs = null;
        var currentJournal = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        PrepareSegmentForSequenceScan(options, currentJournal, ref repairs);

        // The roll target may have been pre-created before its manifest publish; a torn
        // leftover there must not fail the sequence scan, so repair it the same way.
        PrepareSegmentForSequenceScan(options, currentJournal + 1, ref repairs);
        return repairs ?? [];
    }

    /// <summary>
    /// Deletes orphaned roll-target temp files left by a crash between header staging and atomic publication.
    /// Best-effort: temp files are invisible to segment enumeration and are truncated on reuse, so a cleanup
    /// failure must not fail startup.
    /// </summary>
    /// <param name="dataDir">Persistence directory containing journal segment files.</param>
    internal static void DeleteOrphanedRollTempFiles(string dataDir)
    {
        string[] files;
        try
        {
            if (!Directory.Exists(dataDir))
                return;

            files = Directory.GetFiles(dataDir, $"{FilePrefixes.Journal}*.tmp", SearchOption.TopDirectoryOnly);
        }
        catch (Exception ex) when (ex is ArgumentException or IOException or UnauthorizedAccessException)
        {
            return;
        }

        foreach (var file in files)
            _ = FileEx.TryDeleteFile(file);
    }

    private static void PrepareSegmentForSequenceScan(PersistenceOptions options, int segmentIndex, ref List<JournalRepair>? repairs)
    {
        var path = JournalReadPath.BuildSegmentPath(options.DataDir, segmentIndex);
        if (!File.Exists(path))
            return;

        using var writer = JournalSegmentWriterFactory.Create();
        writer.OpenSegment(path, true);
        if (writer.Length == 0)
            return;

        RepairTornTailIfNeeded(writer, path, ref repairs);
    }

    private static (HeaderState Header, long ValidLength) AnalyzeSegment(string path)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan);
        var length = RandomAccess.GetLength(handle);
        var header = ClassifyHeader(handle, path, length);
        return header == HeaderState.TornCreation ? (header, 0) : (header, FindValidLength(handle, path, length));
    }

    private static HeaderState ClassifyDamagedHeader(SafeFileHandle handle, string path, long length)
    {
        if (length <= JournalFraming.FileHeaderSize || ContainsOnlyZeros(handle, JournalFraming.FileHeaderSize, length))
            return HeaderState.TornCreation;

        var firstFrame = JournalFrameReader.ReadNext(handle, JournalFraming.FileHeaderSize, out var rentedBuffer, out _);
        if (rentedBuffer != null)
            ArrayPool<byte>.Shared.ReturnCleared(rentedBuffer);

        return firstFrame.Status == JournalFrameReadStatus.Success
            ? HeaderState.Damaged
            : throw new InvalidDataException(
                $"journal segment '{path}' has a damaged file header and no intact first frame at offset {JournalFraming.FileHeaderSize}; startup repair refuses to discard data.");
    }

    /// <summary>
    /// Classifies the segment header. An unsupported non-zero version under the journal magic is never repaired and throws.
    /// A header of a file that is not longer than the header, or is zero after it, is a torn creation. A damaged header in front of an
    /// intact first frame is restorable. Any other damaged header throws.
    /// </summary>
    /// <param name="handle">The segment file handle.</param>
    /// <param name="path">The segment path.</param>
    /// <param name="length">The segment length in bytes.</param>
    /// <returns>The header state.</returns>
    private static HeaderState ClassifyHeader(SafeFileHandle handle, string path, long length)
    {
        try
        {
            JournalFraming.ReadAndValidateSegmentHeader(handle, 0);
            return HeaderState.Valid;
        }
        catch (InvalidDataException) when (!JournalFraming.HasUnsupportedVersion(path))
        {
            return ClassifyDamagedHeader(handle, path, length);
        }
    }

    private static bool ContainsOnlyZeros(SafeFileHandle handle, long start, long length)
    {
        Span<byte> buffer = stackalloc byte[4096];
        var position = start;
        while (position < length)
        {
            var read = RandomAccess.Read(handle, buffer[..int.CreateTruncating(Math.Min(buffer.Length, length - position))], position);
            if (read == 0)
                return true;

            if (buffer[..read].IndexOfAnyExcept<byte>(0) >= 0)
                return false;

            position += read;
        }

        return true;
    }

    private static InvalidDataException CreateCorruptFrameException(string path, long offset, JournalFrameReadStatus status) =>
        new($"journal segment '{path}' has a corrupt frame ({status}) at offset {offset} that is not a torn tail; startup repair refuses to discard data.");

    private static InvalidDataException CreateTopologyDisjointException() => new("journal recovery cannot determine a valid replay start.");

    /// <summary>
    /// Finds the length of the valid prefix of a segment whose header is valid or restorable. Only a torn tail may be cut.
    /// A frame cut by the end of file (truncated header, payload or checksum) is a torn tail. A checksum mismatch is a torn tail only
    /// when every byte after the frame's declared extent (length field plus declared payload plus checksum) is zero, which is what a last
    /// frame partially written into a pre-sized file looks like. An oversized declared length is a torn tail only when every byte after the
    /// 4-byte length field is zero. Anything else, including a bad frame followed by any non-zero byte beyond its extent, throws because a
    /// valid frame could follow.
    /// </summary>
    /// <param name="handle">The segment file handle.</param>
    /// <param name="path">The segment path used in diagnostics.</param>
    /// <param name="length">The segment length in bytes.</param>
    /// <returns>The offset at which the file must end.</returns>
    /// <exception cref="InvalidDataException">Thrown when the damage is not a provable torn tail.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the frame reader reports an unknown status.</exception>
    private static long FindValidLength(SafeFileHandle handle, string path, long length)
    {
        long offset = JournalFraming.FileHeaderSize;
        while (true)
        {
            var read = JournalFrameReader.ReadNext(handle, offset, out var rentedBuffer, out var payloadLength);
            if (rentedBuffer != null)
                ArrayPool<byte>.Shared.ReturnCleared(rentedBuffer);

            switch (read.Status)
            {
                case JournalFrameReadStatus.Success:
                    offset = read.NextFrameOffset;
                    break;
                case JournalFrameReadStatus.EndOfFile:
                case JournalFrameReadStatus.TruncatedHeader:
                case JournalFrameReadStatus.TruncatedPayload:
                case JournalFrameReadStatus.TruncatedChecksum:
                    return offset;
                case JournalFrameReadStatus.ChecksumMismatch:
                    return ContainsOnlyZeros(handle, offset + JournalFrameEnvelope.TotalLength(payloadLength), length)
                        ? offset
                        : throw CreateCorruptFrameException(path, offset, read.Status);
                case JournalFrameReadStatus.OversizedFrame:
                    return ContainsOnlyZeros(handle, offset + JournalFrameEnvelope.HeaderSize, length)
                        ? offset
                        : throw CreateCorruptFrameException(path, offset, read.Status);
                default:
                    throw new InvalidOperationException($"Unsupported frame read status {read.Status}.");
            }
        }
    }

    private static (int FirstAvailableSegment, int LastAvailableSegment) ProbeAvailableSegments(string dataDir)
    {
        var firstAvailableSegment = 0;
        var lastAvailableSegment = 0;
        foreach (var segment in JournalReadPath.EnumerateSegments(dataDir, 1))
        {
            if (firstAvailableSegment == 0)
                firstAvailableSegment = segment.Index;

            lastAvailableSegment = segment.Index;
        }

        return (firstAvailableSegment, lastAvailableSegment);
    }

    private static void RepairTornTailIfNeeded(IJournalSegmentWriter writer, string path, ref List<JournalRepair>? repairs)
    {
        var originalLength = writer.Length;
        var (header, validLength) = AnalyzeSegment(path);
        if (header == HeaderState.TornCreation)
        {
            writer.Truncate(0);
            WriteFreshFileHeader(writer);
            writer.FlushToDisk();
            (repairs ??= []).Add(new JournalRepair(path, JournalRepairKind.TornCreationHeaderRewritten, originalLength, originalLength));
            return;
        }

        if (header == HeaderState.Damaged)
        {
            WriteFreshFileHeader(writer);
            writer.FlushToDisk();
            (repairs ??= []).Add(new JournalRepair(path, JournalRepairKind.HeaderRestored, originalLength, 0));
        }

        if (validLength == originalLength)
            return;

        writer.Truncate(validLength);
        writer.FlushToDisk();
        (repairs ??= []).Add(new JournalRepair(path, JournalRepairKind.TornTailTruncated, originalLength, originalLength - validLength));
    }

    private static ulong ResolveBaselineNextSequence(State manifest)
    {
        var next = manifest.NextSequence == 0UL ? 1UL : manifest.NextSequence;
        if (manifest.LastSnapshot?.LastAppliedSequence is { } lastApplied && lastApplied >= next)
            next = lastApplied + 1UL;

        return next;
    }

    private static void ThrowIfJournalOnlyTopologyDisjoint(int manifestCurrentJournal, int firstAvailableSegment, int lastAvailableSegment)
    {
        if (firstAvailableSegment == 0)
        {
            if (manifestCurrentJournal != 1)
                throw CreateTopologyDisjointException();

            return;
        }

        if (lastAvailableSegment < manifestCurrentJournal)
            throw CreateTopologyDisjointException();
    }

    private static void WriteFreshFileHeader(IJournalSegmentWriter writer)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        writer.Write(header, 0);
    }
}
