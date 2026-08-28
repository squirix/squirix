namespace Squirix.Server.Storage;

/// <summary>Hard and default segment capacity limits.</summary>
internal static class JournalSegmentLimits
{
    public const int DefaultMaxSegmentCount = 32;
    public const int DefaultMaxSegmentMb = 64;

    public const int DefaultMaxTotalBytesMb = 2048;

    public const int HardMaxSegmentCount = 1024;

    public const int HardMaxSegmentMb = 4096;

    public const int HardMaxTotalBytesMb = 65536;

    /// <summary>Soft high-water mark as a percent of <see cref="DefaultMaxTotalBytesMb" /> / configured max (details only).</summary>
    public const int HighWaterPercent = 80;

    /// <summary>Hard upper bound on a single journal frame payload length, in bytes.</summary>
    /// <remarks>
    /// A frame can never exceed its segment. The bound is kept safely below the ~2&#160;GB that would make
    /// ArrayPool&lt;T&gt;.Shared.Rent attempt an OOM-sized allocation, while remaining far above any
    /// legitimate journal record payload. The on-disk length field is a signed 32-bit integer, so a stored
    /// value larger than this can never be a real frame and is treated as a corrupt header.
    /// </remarks>
    public const int MaxFramePayloadBytes = 512 * 1024 * 1024;
}
