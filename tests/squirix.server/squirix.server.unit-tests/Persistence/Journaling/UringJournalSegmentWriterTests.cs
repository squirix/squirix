using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Platform;
using Squirix.Server.Storage.Journaling.Platform.IoUring;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Linux io_uring segment writer smoke tests (ubuntu-latest CI).</summary>
public sealed class UringJournalSegmentWriterTests
{
    /// <summary>When io_uring is available, Uring backend writes and fsyncs through the ring.</summary>
    [Fact]
    public async Task UringWriterRoundTripsWhenSupported()
    {
        if (!OperatingSystem.IsLinux())
            return;

        if (!IoUringAvailability.IsSupported)
            return;

        using var dir = new TempDirectory("journal-uring");
        var path = Path.Combine(dir.Path, "segment.bin");
        await using var writer = JournalSegmentWriterFactory.Create(JournalPlatformBackend.Uring);
        var uringWriter = Assert.IsType<UringJournalSegmentWriter>(writer);
        uringWriter.OpenSegment(path, false);
        Assert.True(uringWriter.UsesIoUring);

        var payload = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        uringWriter.Write(payload, 0);
        uringWriter.Fsync();

        using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.Read, FileOptions.Asynchronous);
        var read = new byte[payload.Length];
        _ = await RandomAccess.ReadAsync(handle, read, 0, TestContext.Current.CancellationToken).ConfigureAwait(true);
        Assert.Equal(payload, read);
    }

    /// <summary>Auto backend on Linux selects Uring when the kernel exposes io_uring.</summary>
    [Fact]
    public async Task AutoBackendSelectsUringOnLinuxWhenSupported()
    {
        if (!OperatingSystem.IsLinux())
            return;

        if (!IoUringAvailability.IsSupported)
            return;

        await using var writer = JournalSegmentWriterFactory.Create(JournalPlatformBackend.Auto);
        _ = Assert.IsType<UringJournalSegmentWriter>(writer);
    }
}
