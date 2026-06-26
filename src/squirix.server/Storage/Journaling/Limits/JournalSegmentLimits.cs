namespace Squirix.Server.Storage.Journaling.Limits;

/// <summary>Hard and default segment capacity limits.</summary>
internal static class JournalSegmentLimits
{
    public const int DefaultMaxSegmentMb = 64;

    public const int HardMaxSegmentMb = 4096;

    public const int DefaultMaxSegmentCount = 32;

    public const int HardMaxSegmentCount = 1024;

    public const int DefaultMaxTotalBytesMb = 2048;

    public const int HardMaxTotalBytesMb = 65536;
}
