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
        await File.WriteAllBytesAsync(filePath, [1, 2, 3], cancellationToken);

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
        await File.WriteAllBytesAsync(tempPath, [7, 8, 9], cancellationToken);

        _ = await Assert.That(FileEx.PublishFile(tempPath, finalPath)).IsTrue();
        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
        _ = await Assert.That(File.Exists(finalPath)).IsTrue();
        var finalBytes = await File.ReadAllBytesAsync(finalPath, cancellationToken);
        await SequenceAssert.Equal<byte>([7, 8, 9], finalBytes);
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
        await File.WriteAllBytesAsync(finalPath, [10, 20], cancellationToken);
        await File.WriteAllBytesAsync(tempPath, [30, 40], cancellationToken);

        _ = await Assert.That(FileEx.PublishFile(tempPath, finalPath, backupPath)).IsTrue();

        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
        _ = await Assert.That(File.Exists(finalPath)).IsTrue();
        _ = await Assert.That(File.Exists(backupPath)).IsTrue();
        var finalBytes = await File.ReadAllBytesAsync(finalPath, cancellationToken);
        await SequenceAssert.Equal<byte>([30, 40], finalBytes);
        var backupBytes = await File.ReadAllBytesAsync(backupPath, cancellationToken);
        await SequenceAssert.Equal<byte>([10, 20], backupBytes);
    }
}
