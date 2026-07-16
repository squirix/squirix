namespace Squirix.Server.Storage.Journaling;

internal enum JournalFrameReadStatus
{
    /// <summary>The frame header, payload, and checksum were read and validated successfully.</summary>
    Success = 0,

    /// <summary>No more bytes were available at the requested frame offset.</summary>
    EndOfFile = 1,

    /// <summary>The 4-byte frame length header was cut before completion.</summary>
    TruncatedHeader = 2,

    /// <summary>The payload bytes ended before the declared length was fully available.</summary>
    TruncatedPayload = 3,

    /// <summary>The trailing 4-byte checksum footer was cut before completion.</summary>
    TruncatedChecksum = 4,

    /// <summary>The stored checksum footer does not match the payload bytes.</summary>
    ChecksumMismatch = 5,

    /// <summary>The declared payload length exceeds the supported in-memory frame size.</summary>
    OversizedFrame = 6,
}
