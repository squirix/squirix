using System;
using Squirix.Server.Storage.Journaling.Platform;
using Squirix.Server.Storage.Journaling.Platform.IoUring;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Creates <see cref="IJournalSegmentWriter"/> instances for Pipelined.</summary>
internal static class JournalSegmentWriterFactory
{
    public static IJournalSegmentWriter Create(JournalPlatformBackend backend) => backend switch
    {
        JournalPlatformBackend.RandomAccess => new RandomAccessJournalSegmentWriter(),
        JournalPlatformBackend.Uring when OperatingSystem.IsLinux() => new UringJournalSegmentWriter(),
        JournalPlatformBackend.Uring => throw new PlatformNotSupportedException("io_uring journal segment writer is only supported on Linux."),
        JournalPlatformBackend.Auto when OperatingSystem.IsLinux() => CreateUringIfAvailable(),
        _ => new RandomAccessJournalSegmentWriter(),
    };

    private static IJournalSegmentWriter CreateUringIfAvailable()
    {
        if (OperatingSystem.IsLinux() && IoUringAvailability.IsSupported)
            return new UringJournalSegmentWriter();

        return new RandomAccessJournalSegmentWriter();
    }
}
