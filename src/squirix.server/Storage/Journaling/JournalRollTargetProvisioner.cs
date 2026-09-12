using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Durably provisions the next journal segment before its roll manifest is published, so a crash can
/// never leave the manifest ahead of the last available segment (issue #439). All members run on the
/// dedicated <c language="csharp">squirix-journal-io</c> thread.
/// </summary>
[Immutable]
internal sealed class JournalRollTargetProvisioner
{
    private readonly IJournalEventLoopState _owner;
    private readonly IJournalEventLoopRollState _roll;

    internal JournalRollTargetProvisioner(IJournalEventLoopState owner, IJournalEventLoopRollState roll)
    {
        _owner = owner;
        _roll = roll;
    }

    /// <summary>Builds the roll target path for the segment following the current one.</summary>
    /// <returns>Absolute path of the roll target segment file.</returns>
    internal string BuildRollTargetPath() => JournalReadPath.BuildSegmentPath(_owner.Options.DataDir, _roll.CurrentSegmentIndex + 1);

    /// <summary>Ensures the roll target segment exists durably, creating or replacing it atomically when needed.</summary>
    /// <param name="targetSegmentIndex">One-based index of the roll target segment.</param>
    /// <param name="targetPath">Absolute path of the roll target segment file.</param>
    internal void PrepareRollTargetSegment(int targetSegmentIndex, string targetPath)
    {
        // A valid pre-created target (crash after a previous pre-create, before its manifest publish)
        // is reused as-is: startup stats already counted it, and replacing it would needlessly churn
        // the disk.
        if (HasUsableRollTargetHeader(targetPath))
            return;

        long? preExistingLength = null;
        try
        {
            if (File.Exists(targetPath))
                preExistingLength = new FileInfo(targetPath).Length;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            preExistingLength = null;
        }

        PublishRollTargetHeader(targetSegmentIndex, targetPath);

        if (preExistingLength == null)
        {
            _owner.AddJournalTotalBytes(JournalFraming.FileHeaderSize);
            _roll.IncrementJournalSegmentCount();
        }
        else
        {
            // Replaced a torn or empty leftover: the file itself was already counted, only its byte
            // delta is new.
            _owner.AddJournalTotalBytes(JournalFraming.FileHeaderSize - preExistingLength.Value);
        }
    }

    /// <summary>Gets the current length of the roll target segment file.</summary>
    /// <returns>File length in bytes, or <see langword="null"/> when the target does not exist or cannot be statted.</returns>
    internal long? GetRollTargetExistingLength()
    {
        try
        {
            var targetPath = BuildRollTargetPath();
            return !File.Exists(targetPath) ? null : new FileInfo(targetPath).Length;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }

    private static bool HasUsableRollTargetHeader(string targetPath)
    {
        long length;
        try
        {
            if (!File.Exists(targetPath))
                return false;

            length = new FileInfo(targetPath).Length;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return false;
        }

        // A longer file already holds content: never replace it here, reuse as-is. Startup recovery
        // validated its header before any roll could run, and nothing else writes the file while appends
        // are deferred, so revalidating here would add I/O without changing the only safe action.
        // (see PrepareRollTargetSegment).
        if (length > JournalFraming.FileHeaderSize)
            return true;

        if (length != JournalFraming.FileHeaderSize)
            return false;

        try
        {
            using var handle = File.OpenHandle(targetPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete, FileOptions.SequentialScan);
            JournalFraming.ReadAndValidateSegmentHeader(handle, 0);
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidDataException)
        {
            return false;
        }
    }

    private void PublishRollTargetHeader(int targetSegmentIndex, string targetPath)
    {
        // Stage the header under a temp name and publish it atomically: a crash must never leave a
        // partially-written header under the final segment name, because a 1..FileHeaderSize-1 byte
        // trailing segment fails recovery replay. The temp suffix keeps it invisible to enumeration.
        // The active writer is deliberately untouched here; it stays on the old segment until the
        // manifest publish succeeds, so a failed roll leaves writer, path, and offsets consistent.
        var tmpPath = JournalReadPath.BuildRollTempPath(_owner.Options.DataDir, targetSegmentIndex);
        WriteRollTargetHeaderFile(tmpPath);
        _ = FileEx.PublishFile(tmpPath, targetPath);
    }

    private void WriteRollTargetHeaderFile(string tmpPath)
    {
        using var writer = JournalSegmentWriterFactory.Create(_owner.Options.JournalPlatformBackend);
        writer.OpenSegment(tmpPath, false);
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        writer.Write(header, 0);
        writer.Fsync();
    }
}
