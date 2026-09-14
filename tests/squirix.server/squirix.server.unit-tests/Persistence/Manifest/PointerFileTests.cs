using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for shared <c language="csharp">man-current</c> pointer reads used after abrupt shutdown.</summary>
[Immutable]
public sealed class PointerFileTests : IsolatedStorageTestBase
{
    /// <summary>Shared-mode pointer reads succeed while a writer-compatible handle remains open.</summary>
    [Fact]
    public void SharedReadSucceedsWithOpenWriterHandle()
    {
        var path = Path.Join(Dir, "man-current");
        Span<byte> pointer = stackalloc byte[Pointer.Size];
        Pointer.Write(pointer, 7);
        using (var create = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.None))
            RandomAccess.Write(create, pointer, 0);

        using var writer = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, PointerFile.CompatibleShare);
        Assert.Equal(7, PointerFile.ReadIndex(path));
    }

    /// <summary>
    /// The post-abrupt-shutdown lease wait must cover the <c language="csharp">man-current.next</c> staging file, not only
    /// <c language="csharp">man-current</c>: a draining writer handle on the staging file is what blocks offline compact and
    /// recovery on Windows after a force-kill style shutdown (issue #396).
    /// </summary>
    [Fact]
    public async Task LeaseWaitHonorsManifestStagingHandle()
    {
        if (!OperatingSystem.IsWindows())
            Assert.Skip("FileShare enforcement is Windows-specific.");

        var currentPath = Path.Join(Dir, "man-current");
        var stagingPath = Path.Join(Dir, "man-current.next");
        await File.WriteAllBytesAsync(currentPath, new byte[Pointer.Size], DefaultCancellationToken);
        await File.WriteAllBytesAsync(stagingPath, new byte[Pointer.Size], DefaultCancellationToken);

        var held = File.OpenHandle(stagingPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        try
        {
            var wait = JournalSegmentLeaseWait.WaitForReleasedAsync(Dir, DefaultCancellationToken);
            _ = wait.ContinueWith(static task => _ = task.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);

            // While the staging file is still held, the wait must keep polling instead of reporting release.
            await Task.Delay(TimeSpan.FromMilliseconds(400), TimeProvider.System, DefaultCancellationToken);
            Assert.False(wait.IsCompleted);

            held.Dispose();
            await wait.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, DefaultCancellationToken);
        }
        finally
        {
            held.Dispose();
        }
    }
}
