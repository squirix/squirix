using System;

namespace Squirix.Server.Storage.Journaling;

internal static class JournalFrameDiagnostics
{
    internal static string DescribeReadFailure(JournalFrameReadResult read) => read.Status switch
    {
        JournalFrameReadStatus.Success => throw new ArgumentOutOfRangeException(nameof(read), read.Status, "Success is not a failure status."),
        JournalFrameReadStatus.TruncatedHeader => $"truncated journal frame header at offset {read.FrameOffset}",
        JournalFrameReadStatus.TruncatedPayload => $"truncated journal frame payload at offset {read.FrameOffset}",
        JournalFrameReadStatus.TruncatedChecksum => $"truncated journal frame CRC footer at offset {read.FrameOffset}",
        JournalFrameReadStatus.ChecksumMismatch => $"journal frame CRC mismatch at offset {read.FrameOffset}",
        JournalFrameReadStatus.OversizedFrame => $"declared journal payload length exceeds supported maximum at offset {read.FrameOffset}",
        JournalFrameReadStatus.EndOfFile => $"unexpected journal frame EOF at offset {read.FrameOffset}",
        _ => throw new ArgumentOutOfRangeException(nameof(read), read.Status, "Unsupported journal frame read status."),
    };
}
