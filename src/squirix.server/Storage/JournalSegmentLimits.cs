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
}
