using System;

namespace Squirix.Server.Storage.Journaling.PipelinedWal;

/// <summary>Creates <see cref="Platform.IWalSegmentWriter"/> instances for PipelinedWal.</summary>
internal static class WalSegmentWriterFactory
{
    public static Platform.IWalSegmentWriter Create(WalPlatformBackend backend) => backend switch
    {
        WalPlatformBackend.RandomAccess => new Platform.RandomAccessWalSegmentWriter(),
        WalPlatformBackend.Uring when OperatingSystem.IsLinux() => new Platform.UringWalSegmentWriter(),
        WalPlatformBackend.Uring => throw new PlatformNotSupportedException("io_uring WAL writer is only supported on Linux."),
        WalPlatformBackend.Auto when OperatingSystem.IsLinux() => CreateUringIfAvailable(),
        _ => new Platform.RandomAccessWalSegmentWriter(),
    };

    private static Platform.IWalSegmentWriter CreateUringIfAvailable()
    {
        // Auto-select io_uring on Linux when available; RandomAccess otherwise.
        if (OperatingSystem.IsLinux())
            return new Platform.UringWalSegmentWriter();

        return new Platform.RandomAccessWalSegmentWriter();
    }
}
