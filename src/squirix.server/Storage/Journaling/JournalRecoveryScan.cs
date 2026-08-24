using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
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
        ThrowIfJournalOnlyTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);

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

    internal static async Task PrepareActiveSegmentForSequenceScanAsync(State manifest, PersistenceOptions options, CancellationToken cancellationToken)
    {
        var path = JournalReadPath.BuildSegmentPath(options.DataDir, manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal);
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

    private static long ComputeValidLength(SafeFileHandle handle)
    {
        var length = RandomAccess.GetLength(handle);
        if (length == 0)
            return 0;

        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        long offset = 0;
        if (!HandleEx.TryReadExact(handle, header, ref offset))
            throw new InvalidDataException("journal segment has a truncated file header.");

        JournalFraming.EnsureSegmentHeaderSupported(header);

        long validLength = JournalFraming.FileHeaderSize;
        while (true)
        {
            var read = JournalFrameReader.ReadNext(handle, validLength, out var rentedBuffer, out _);
            if (read.Status is JournalFrameReadStatus.EndOfFile or not JournalFrameReadStatus.Success)
                return validLength;

            validLength = read.NextFrameOffset;
            if (rentedBuffer != null)
                ArrayPool<byte>.Shared.ReturnCleared(rentedBuffer);
        }
    }

    private static InvalidDataException CreateJournalTopologyDisjointForSequenceInit() => new("journal recovery cannot determine a valid replay start.");

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

    private static async Task<long> ReadValidSegmentLengthAsync(string path, CancellationToken cancellationToken)
    {
        var handle = File.OpenHandle(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.SequentialScan);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ComputeValidLength(handle);
        }
        finally
        {
            handle.Dispose();
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

    private static ulong ResolveBaselineNextSequence(State manifest)
    {
        var next = manifest.NextSequence == 0UL ? 1UL : manifest.NextSequence;
        if (manifest.LastSnapshot?.LastAppliedSequence is { } lastApplied && lastApplied >= next)
            next = lastApplied + 1UL;

        return next;
    }

    private static void ThrowIfJournalOnlyTopologyDisjointForSequenceInit(int manifestCurrentJournal, int firstAvailableSegment, int lastAvailableSegment)
    {
        if (firstAvailableSegment == 0)
        {
            if (manifestCurrentJournal != 1)
                throw CreateJournalTopologyDisjointForSequenceInit();

            return;
        }

        if (lastAvailableSegment < manifestCurrentJournal)
            throw CreateJournalTopologyDisjointForSequenceInit();
    }

    private static void WriteFreshFileHeader(IJournalSegmentWriter writer)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        writer.Write(header, 0);
    }
}
