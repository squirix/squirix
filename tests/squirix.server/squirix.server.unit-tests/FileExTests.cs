using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for <see cref="FileEx" /> directory flush helpers.</summary>
[Immutable]
public sealed class FileExTests : ServerUnitTestBase
{
    /// <summary>FlushDirectoryEntry succeeds on an existing file and flushes the parent directory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FlushDirectoryEntrySucceedsForFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-fileex-flush");
        var filePath = Path.Join(dir, "test.bin");
        await File.WriteAllBytesAsync(filePath, ReadOnlyMemory<byte>.Of(1, 2, 3), cancellationToken);

        FileEx.FlushDirectoryEntry(filePath);

        _ = await Assert.That(File.Exists(filePath)).IsTrue();
    }

    /// <summary>PublishFile moves a temp file to its final location and flushes the directory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishFileMovesAndFlushes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-fileex-publish");
        var tempPath = Path.Join(dir, "temp.bin");
        var finalPath = Path.Join(dir, "final.bin");
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(7, 8, 9), cancellationToken);

        _ = await Assert.That(FileEx.PublishFile(tempPath, finalPath, TimeProvider.System)).IsTrue();
        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
        _ = await Assert.That(File.Exists(finalPath)).IsTrue();
        var finalBytes = await File.ReadAllBytesAsync(finalPath, cancellationToken);
        await SequenceAssert.EqualAsync<byte>([7, 8, 9], finalBytes);
    }

    /// <summary>PublishFile with backup replaces existing final and produces backup copy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishFileReplaceWithBackup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-fileex-replace");
        var tempPath = Path.Join(dir, "temp.bin");
        var finalPath = Path.Join(dir, "final.bin");
        var backupPath = Path.Join(dir, "backup.bin");
        await File.WriteAllBytesAsync(finalPath, ReadOnlyMemory<byte>.Of(10, 20), cancellationToken);
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(30, 40), cancellationToken);

        _ = await Assert.That(FileEx.PublishFile(tempPath, finalPath, TimeProvider.System, backupPath)).IsTrue();

        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
        _ = await Assert.That(File.Exists(finalPath)).IsTrue();
        _ = await Assert.That(File.Exists(backupPath)).IsTrue();
        var finalBytes = await File.ReadAllBytesAsync(finalPath, cancellationToken);
        await SequenceAssert.EqualAsync<byte>([30, 40], finalBytes);
        var backupBytes = await File.ReadAllBytesAsync(backupPath, cancellationToken);
        await SequenceAssert.EqualAsync<byte>([10, 20], backupBytes);
    }

    /// <summary>PublishFile waits out a transient Windows sharing violation and then publishes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishFileWaitsOutSharingViolation(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var dir = new TempDirectory("squirix-fileex-sharing");
        var tempPath = Path.Join(dir, "temp.bin");
        var finalPath = Path.Join(dir, "final.bin");
        await File.WriteAllBytesAsync(finalPath, ReadOnlyMemory<byte>.Of(1, 2), cancellationToken);
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(3, 4), cancellationToken);
        var holder = File.OpenHandle(finalPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        try
        {
            var clock = new DueTimerClock(FileEx.PublishRetryDelay);
            var publish = StartPublishAsync(tempPath, finalPath, clock, cancellationToken);
            if (ReferenceEquals(await Task.WhenAny(publish, clock.TimerCreated.WaitAsync(cancellationToken)), publish))
            {
                _ = await publish;
                Assert.Fail("Publish completed without waiting for a retry.");
            }

            holder.Dispose();
            clock.Advance(FileEx.PublishRetryDelay);

            _ = await Assert.That(await publish).IsTrue();
        }
        finally
        {
            holder.Dispose();
        }

        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
        var finalBytes = await File.ReadAllBytesAsync(finalPath, cancellationToken);
        await SequenceAssert.EqualAsync<byte>([3, 4], finalBytes);
    }

    /// <summary>PublishFile gives up after the bounded number of attempts and leaves temp and final untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishFileFailsAfterBoundedRetries(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var dir = new TempDirectory("squirix-fileex-bounded");
        var tempPath = Path.Join(dir, "temp.bin");
        var finalPath = Path.Join(dir, "final.bin");
        await File.WriteAllBytesAsync(finalPath, ReadOnlyMemory<byte>.Of(1, 2), cancellationToken);
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(3, 4), cancellationToken);
        using var holder = File.OpenHandle(finalPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var clock = new DueTimerClock(FileEx.PublishRetryDelay);
        var publish = StartPublishAsync(tempPath, finalPath, clock, cancellationToken);

        for (var i = 0; i < FileEx.PublishAttempts - 1 && !publish.IsCompleted; i++)
        {
            if (ReferenceEquals(await Task.WhenAny(publish, clock.TimerCreated.WaitAsync(cancellationToken)), publish))
                break;

            clock.Advance(FileEx.PublishRetryDelay);
        }

        var exception = await NodeAsyncAssert.ThrowsAsync<IOException>(publish);

        _ = await Assert.That(exception.HResult is -2147024864 or -2147023721 or -2147023720).IsTrue();
        _ = await Assert.That(clock.TimerCreated.CurrentCount).IsEqualTo(0);
        _ = await Assert.That(File.Exists(tempPath)).IsTrue();
        var finalBytes = await File.ReadAllBytesAsync(finalPath, cancellationToken);
        await SequenceAssert.EqualAsync<byte>([1, 2], finalBytes);
    }

    /// <summary>PublishFile over a destination held open by a reader needs no retry on Unix.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishFileOpenReaderNoRetryOnUnix(CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
            return;

        using var dir = new TempDirectory("squirix-fileex-unix-reader");
        var tempPath = Path.Join(dir, "temp.bin");
        var finalPath = Path.Join(dir, "final.bin");
        await File.WriteAllBytesAsync(finalPath, ReadOnlyMemory<byte>.Of(1, 2), cancellationToken);
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(3, 4), cancellationToken);
        using var holder = File.OpenHandle(finalPath, FileMode.Open, FileAccess.Read, FileShare.Read);
        var clock = new DueTimerClock(FileEx.PublishRetryDelay);

        _ = await Assert.That(FileEx.PublishFile(tempPath, finalPath, clock)).IsTrue();

        _ = await Assert.That(clock.TimerCreated.CurrentCount).IsEqualTo(0);
        var finalBytes = await File.ReadAllBytesAsync(finalPath, cancellationToken);
        await SequenceAssert.EqualAsync<byte>([3, 4], finalBytes);
    }

    /// <summary>PublishFile surfaces a missing temp file immediately without waiting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishFileDoesNotRetryMissingTemp(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-fileex-missing-temp");
        var tempPath = Path.Join(dir, "temp.bin");
        var finalPath = Path.Join(dir, "final.bin");
        await File.WriteAllBytesAsync(finalPath, ReadOnlyMemory<byte>.Of(1, 2), cancellationToken);
        var clock = new DueTimerClock(FileEx.PublishRetryDelay);
        var publish = StartPublishAsync(tempPath, finalPath, clock, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(publish);

        _ = await Assert.That(clock.TimerCreated.CurrentCount).IsEqualTo(0);
    }

    /// <summary>PublishFile surfaces an access denied failure immediately without waiting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PublishFileDoesNotRetryAccessDenied(CancellationToken cancellationToken)
    {
        if (!OperatingSystem.IsWindows())
            return;

        using var dir = new TempDirectory("squirix-fileex-access-denied");
        var tempPath = Path.Join(dir, "temp.bin");
        var finalPath = Path.Join(dir, "final.bin");
        await File.WriteAllBytesAsync(finalPath, ReadOnlyMemory<byte>.Of(1, 2), cancellationToken);
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(3, 4), cancellationToken);
        File.SetAttributes(finalPath, FileAttributes.ReadOnly);
        try
        {
            var clock = new DueTimerClock(FileEx.PublishRetryDelay);
            var publish = StartPublishAsync(tempPath, finalPath, clock, cancellationToken);

            _ = await NodeAsyncAssert.ThrowsAsync<UnauthorizedAccessException>(publish);

            _ = await Assert.That(clock.TimerCreated.CurrentCount).IsEqualTo(0);
        }
        finally
        {
            File.SetAttributes(finalPath, FileAttributes.Normal);
        }
    }

    private static Task<bool> StartPublishAsync(string tempPath, string finalPath, TimeProvider clock, CancellationToken cancellationToken)
    {
        return Task.Factory.StartNew(
            static state => state is PublishRequest request
                ? FileEx.PublishFile(request.TempPath, request.FinalPath, request.Clock)
                : throw new InvalidOperationException("Unexpected publish state."),
            new PublishRequest(tempPath, finalPath, clock),
            cancellationToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default);
    }

    private sealed record PublishRequest(string TempPath, string FinalPath, TimeProvider Clock);
}
