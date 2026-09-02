using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for <see cref="FileEx" /> directory flush helpers.</summary>
[Immutable]
public sealed class FileExTests : ServerUnitTestBase
{
    /// <summary>FlushDirectoryEntry succeeds on an existing file and flushes the parent directory.</summary>
    [Fact]
    public void FlushDirectoryEntrySucceedsForFile()
    {
        using var dir = new TempDirectory("squirix-fileex-flush");
        var filePath = Path.Join(dir.Path, "test.bin");
        File.WriteAllBytes(filePath, [1, 2, 3]);

        FileEx.FlushDirectoryEntry(filePath);

        Assert.True(File.Exists(filePath));
    }

    /// <summary>PublishFile moves a temp file to its final location and flushes the directory.</summary>
    [Fact]
    public void PublishFileMovesAndFlushes()
    {
        using var dir = new TempDirectory("squirix-fileex-publish");
        var tempPath = Path.Join(dir.Path, "temp.bin");
        var finalPath = Path.Join(dir.Path, "final.bin");
        File.WriteAllBytes(tempPath, [7, 8, 9]);

        FileEx.PublishFile(tempPath, finalPath);

        Assert.False(File.Exists(tempPath));
        Assert.True(File.Exists(finalPath));
        Assert.Equal([7, 8, 9], File.ReadAllBytes(finalPath));
    }

    /// <summary>PublishFile with backup replaces existing final and produces backup copy.</summary>
    [Fact]
    public void PublishFileReplaceWithBackup()
    {
        using var dir = new TempDirectory("squirix-fileex-replace");
        var tempPath = Path.Join(dir.Path, "temp.bin");
        var finalPath = Path.Join(dir.Path, "final.bin");
        var backupPath = Path.Join(dir.Path, "backup.bin");
        File.WriteAllBytes(finalPath, [10, 20]);
        File.WriteAllBytes(tempPath, [30, 40]);

        FileEx.PublishFile(tempPath, finalPath, backupPath);

        Assert.False(File.Exists(tempPath));
        Assert.True(File.Exists(finalPath));
        Assert.True(File.Exists(backupPath));
        Assert.Equal([30, 40], File.ReadAllBytes(finalPath));
        Assert.Equal([10, 20], File.ReadAllBytes(backupPath));
    }
}
