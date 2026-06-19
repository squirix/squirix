namespace Squirix.Server.Storage.Journaling.PipelinedWal.Limits;

/// <summary>Hard and default segment capacity limits for <see cref="JournalBackend.PipelinedWal"/>.</summary>
internal static class WalSegmentLimits
{
    public const int DefaultMaxSegmentMb = 64;

    public const int HardMaxSegmentMb = 4096;

    public const int DefaultMaxSegmentCount = 32;

    public const int HardMaxSegmentCount = 1024;

    public const int DefaultMaxTotalBytesMb = 2048;

    public const int HardMaxTotalBytesMb = 65536;
}
