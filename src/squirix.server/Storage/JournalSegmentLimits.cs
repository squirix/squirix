using Squirix.Server.Core;

namespace Squirix.Server.Storage;

/// <summary>Hard and default segment capacity limits.</summary>
internal static class JournalSegmentLimits
{
    /// <summary>Default maximum size of a single journal segment, in megabytes.</summary>
    internal const int DefaultMaxSegmentMb = 64;

    /// <summary>Default maximum total journal storage across all segments, in megabytes.</summary>
    internal const int DefaultMaxTotalBytesMb = 2048;

    /// <summary>Hard upper bound on the number of journal segments that can be retained.</summary>
    internal const int HardMaxSegmentCount = 1024;

    /// <summary>Hard upper bound on a single journal segment size, in megabytes.</summary>
    internal const int HardMaxSegmentMb = 4096;

    /// <summary>Hard upper bound on total journal storage across all segments, in megabytes.</summary>
    internal const int HardMaxTotalBytesMb = 65536;

    /// <summary>Soft high-water mark as a percent of <see cref="DefaultMaxTotalBytesMb" /> / configured max (details only).</summary>
    internal const int HighWaterPercent = 80;

    /// <summary>Hard upper bound on a single journal frame payload length, in bytes.</summary>
    /// <remarks>
    /// A frame can never exceed its segment. The bound is kept safely below the ~2&#160;GB that would make
    /// ArrayPool&lt;T&gt;.Shared.Rent attempt an OOM-sized allocation, while remaining far above any
    /// legitimate journal record payload. The on-disk length field is a signed 32-bit integer, so a stored
    /// value larger than this can never be a real frame and is treated as a corrupt header.
    /// </remarks>
    internal const int MaxFramePayloadBytes = 512 * 1024 * 1024;

    /// <summary>Default maximum number of journal segments retained before compaction prunes older segments.</summary>
    internal const int DefaultMaxSegmentCount = 32;

    /// <summary>Upper bound on the bytes a journal frame carries besides its largest variable part.</summary>
    /// <remarks>
    /// The fixed record prefix, the frame header and checksum, and the four length-prefixed strings a frame may carry (cache name, key,
    /// operation id and fingerprint, each at most 65 535 bytes) stay below this bound.
    /// </remarks>
    internal const int MaxFrameOverheadBytes = 512 * 1024;

    /// <summary>The smallest segment, in megabytes, that holds the largest journal frame the server writes.</summary>
    /// <remarks>
    /// A frame never spans segments. The largest variable part of a frame is an entry (at most <see cref="EntryLimits.MaxEntrySizeBytes" />)
    /// or a recorded reply, which is at most <see cref="EntryLimits.GrpcMaxSendMessageSizeBytes" /> since a larger one cannot reach the
    /// client; a segment also starts with a 5-byte file header.
    /// </remarks>
    internal const int MinSegmentMb = (SegmentFileHeaderBytes + EntryLimits.GrpcMaxSendMessageSizeBytes + MaxFrameOverheadBytes + BytesPerMb - 1) / BytesPerMb;

    private const int BytesPerMb = 1024 * 1024;

    /// <summary>The magic and version that open every segment file.</summary>
    private const int SegmentFileHeaderBytes = 5;
}
