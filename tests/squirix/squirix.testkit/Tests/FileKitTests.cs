using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.TestKit.Tests;

/// <summary>
/// Unit tests for <see cref="FileKit" />.
/// </summary>
public sealed class FileKitTests : IAsyncLifetime
{
    private string _dir = null!;

    /// <summary>
    /// Missing files return false after validation passes.
    /// </summary>
    [Fact]
    public void ExistsReturnsFalseForMissingFile()
    {
        var missing = PathKit.Combine(_dir, "missing.json");

        Assert.False(FileKit.Exists(missing));
    }

    /// <summary>
    /// Existing files are detected successfully.
    /// </summary>
    [Fact]
    public void ExistsReturnsTrueForExistingFile()
    {
        var path = PathKit.Combine(_dir, "state.json");
        File.WriteAllText(path, "{}");

        Assert.True(FileKit.Exists(path));
    }

    /// <summary>
    /// Windows-reserved file names are rejected.
    /// </summary>
    [Fact]
    public void ExistsThrowsForReservedWindowsName()
    {
        if (!OperatingSystem.IsWindows())
            return;

        var path = PathKit.Combine(_dir, "con.txt");
        _ = Assert.Throws<ArgumentException>(() => FileKit.Exists(path));
    }

    /// <summary>
    /// Wildcard paths are rejected instead of being treated as globs.
    /// </summary>
    [Fact]
    public void ExistsThrowsForWildcardPath()
    {
        var path = PathKit.Combine(false, _dir, "*.json");

        _ = Assert.Throws<ArgumentException>(() => FileKit.Exists(path));
    }

    /// <summary>
    /// Windows COM0 and LPT0 device aliases are rejected.
    /// </summary>
    [Fact]
    public void ExistsThrowsForZeroNumberedReservedWindowsDeviceNames()
    {
        if (!OperatingSystem.IsWindows())
            return;

        _ = Assert.Throws<ArgumentException>(() => FileKit.Exists(PathKit.Combine(_dir, "COM0.txt")));
        _ = Assert.Throws<ArgumentException>(() => FileKit.Exists(PathKit.Combine(_dir, "LPT0.txt")));
    }

    /// <summary>
    /// Paths without a file name are rejected.
    /// </summary>
    [Fact]
    public void ExistsThrowsWhenPathDoesNotIncludeFileName() => Assert.Throws<ArgumentException>(() => FileKit.Exists(_dir + Path.DirectorySeparatorChar));

    /// <summary>
    /// TryDelete removes an existing file and suppresses errors.
    /// </summary>
    [Fact]
    public void TryDeleteRemovesExistingFile()
    {
        var path = PathKit.Combine(_dir, "data.bin");
        File.WriteAllBytes(path, [1, 2, 3]);

        FileKit.TryDelete(path);

        Assert.False(File.Exists(path));
    }

    /// <summary>
    /// WriteAllTextAsync creates a file and persists the provided content.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task WriteAllTextAsyncCreatesFileWithContent()
    {
        var path = PathKit.Combine(_dir, "data-async.txt");

        await FileKit.WriteAllTextAsync(path, "hello", TestContext.Current.CancellationToken);

        Assert.True(FileKit.Exists(path));
        Assert.Equal(5, FileKit.GetLength(path));
    }

    /// <summary>
    /// WriteAllText creates a file and persists the provided content.
    /// </summary>
    [Fact]
    public void WriteAllTextCreatesFileWithContent()
    {
        var path = PathKit.Combine(_dir, "data.txt");

        FileKit.WriteAllText(path, "hello");

        Assert.True(FileKit.Exists(path));
        Assert.Equal(5, FileKit.GetLength(path));
    }

    /// <summary>
    /// Deletes the temporary working directory after each test.
    /// </summary>
    /// <returns>A completed disposal task.</returns>
    public ValueTask DisposeAsync()
    {
        DirectoryKit.TryDeleteDirectory(_dir);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Creates a fresh temporary working directory for each test.
    /// </summary>
    /// <returns>A completed initialization task.</returns>
    public ValueTask InitializeAsync()
    {
        _dir = DirectoryKit.CreateTempDirectory("squirix");
        return ValueTask.CompletedTask;
    }
}
