using System;
using System.Buffers;
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

    internal static void PrepareActiveSegmentForSequenceScan(State manifest, PersistenceOptions options)
    {
        var currentJournal = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        PrepareSegmentForSequenceScan(options, currentJournal);

        // The roll target may have been pre-created before its manifest publish (issue #439); a torn
        // leftover there must not fail the sequence scan, so repair it the same way.
        PrepareSegmentForSequenceScan(options, currentJournal + 1);
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

    private static void PrepareSegmentForSequenceScan(PersistenceOptions options, int segmentIndex)
    {
        var path = JournalReadPath.BuildSegmentPath(options.DataDir, segmentIndex);
        if (!File.Exists(path))
            return;

        using var writer = JournalSegmentWriterFactory.Create(options.JournalPlatformBackend);
        writer.OpenSegment(path, true);
        if (writer.Length == 0)
            return;

        RepairTornTailIfNeeded(writer, path);
    }

    private static long ComputeValidLength(SafeFileHandle handle)
    {
        var length = RandomAccess.GetLength(handle);
        if (length == 0)
            return 0;

        JournalFraming.ReadAndValidateSegmentHeader(handle, 0);
        long validLength = JournalFraming.FileHeaderSize;
        while (true)
        {
            var read = JournalFrameReader.ReadNext(handle, validLength, out var rentedBuffer, out _);
            if (read.Status == JournalFrameReadStatus.EndOfFile || read.Status != JournalFrameReadStatus.Success)
                return validLength;

            validLength = read.NextFrameOffset;
            if (rentedBuffer != null)
                ArrayPool<byte>.Shared.ReturnCleared(rentedBuffer);
        }
    }

    private static InvalidDataException CreateTopologyDisjointException() => new("journal recovery cannot determine a valid replay start.");

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

    private static long ReadValidSegmentLength(string path)
    {
        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan);
        return ComputeValidLength(handle);
    }

    private static void RepairTornTailIfNeeded(IJournalSegmentWriter writer, string path)
    {
        try
        {
            var length = ReadValidSegmentLength(path);
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
