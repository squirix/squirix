using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Exceptions;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Coverage for shared <c language="csharp">man-current</c> pointer reads used after abrupt shutdown.</summary>
[Immutable]
public sealed class PointerFileTests : IsolatedStorageTestBase
{
    /// <summary>
    /// The post-abrupt-shutdown lease wait must cover the <c language="csharp">man-current.next</c> staging file, not only
    /// <c language="csharp">man-current</c>: a draining writer handle on the staging file is what blocks offline compact and
    /// recovery on Windows after a force-kill style shutdown (issue #396).
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="SkipTestException">Thrown when the environment cannot satisfy the test precondition.</exception>
    [Test]
    public async Task LeaseWaitHonorsManifestStagingHandle(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            throw new SkipTestException("FileShare enforcement is Windows-specific.");

        var currentPath = Path.Join(Dir, "man-current");
        var stagingPath = Path.Join(Dir, "man-current.next");
        await File.WriteAllBytesAsync(currentPath, new byte[Pointer.Size], cancellationToken);
        await File.WriteAllBytesAsync(stagingPath, new byte[Pointer.Size], cancellationToken);

        var held = File.OpenHandle(stagingPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        try
        {
            var wait = JournalSegmentLeaseWait.WaitForReleasedAsync(Dir, cancellationToken);
            _ = wait.ContinueWith(static task => _ = task.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);

            // While the staging file is still held, the wait must keep polling instead of reporting release.
            await Task.Delay(TimeSpan.FromMilliseconds(400), TimeProvider.System, cancellationToken);
            _ = await Assert.That(wait.IsCompleted).IsFalse();

            held.Dispose();
            await wait.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        }
        finally
        {
            held.Dispose();
        }
    }

    /// <summary>Shared-mode pointer reads succeed while a writer-compatible handle remains open.</summary>
    [Test]
    public async Task SharedReadSucceedsWithOpenWriterHandle()
    {
        var path = Path.Join(Dir, "man-current");
        WritePointerFile(path, 7);
        using var writer = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, PointerFile.CompatibleShare);
        _ = await Assert.That(PointerFile.ReadIndex(path)).IsEqualTo(7);
        return;

        static void WritePointerFile(string path, int index)
        {
            Span<byte> pointer = stackalloc byte[Pointer.Size];
            Pointer.Write(pointer, index);
            using var create = File.OpenHandle(path, FileMode.Create, FileAccess.Write, FileShare.None);
            RandomAccess.Write(create, pointer, 0);
        }
    }
}
