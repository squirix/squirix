namespace Squirix.Server.Storage.Journaling;

internal enum JournalFrameReadStatus
{
    /// <summary>The frame header, payload, and checksum were read and validated successfully.</summary>
    Success,

    /// <summary>No more bytes were available at the requested frame offset.</summary>
    EndOfFile,

    /// <summary>The 4-byte frame length header was cut before completion.</summary>
    TruncatedHeader,

    /// <summary>The payload bytes ended before the declared length was fully available.</summary>
    TruncatedPayload,

    /// <summary>The trailing 4-byte checksum footer was cut before completion.</summary>
    TruncatedChecksum,

    /// <summary>The stored checksum footer does not match the payload bytes.</summary>
    ChecksumMismatch,

    /// <summary>The declared payload length exceeds the supported in-memory frame size.</summary>
    OversizedFrame,
}
