using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.TestKit.Tests;

/// <summary>
/// Unit tests for <see cref="DirectoryKit" />.
/// </summary>
public sealed class DirectoryKitTests : IAsyncLifetime
{
    private string _dir = null!;

    /// <summary>
    /// Windows COM0 and LPT0 device aliases are rejected.
    /// </summary>
    [Fact]
    public void CreateTempDirectoryThrowsForZeroNumberedReservedWindowsDeviceNames()
    {
        if (!OperatingSystem.IsWindows())
            return;

        _ = Assert.Throws<ArgumentException>(static () => DirectoryKit.CreateTempDirectory("COM0"));
        _ = Assert.Throws<ArgumentException>(static () => DirectoryKit.CreateTempDirectory("LPT0"));
    }

    /// <summary>
    /// Parsed values from matching files are added to the provided sink.
    /// </summary>
    [Fact]
    public void TryCollectFileValuesCollectsParsedValues()
    {
        File.WriteAllText(PathKit.Combine(_dir, "item-000001.dat"), string.Empty);
        File.WriteAllText(PathKit.Combine(_dir, "item-000003.dat"), string.Empty);
        File.WriteAllText(PathKit.Combine(_dir, "other.txt"), string.Empty);

        var sink = new SortedSet<int>();
        DirectoryKit.TryCollectFileValues(
            _dir,
            "item-*.dat",
            sink,
            static (filePath, values) =>
            {
                var name = Path.GetFileNameWithoutExtension(filePath);
                if (name.Length == 11 && name.StartsWith("item-", StringComparison.Ordinal) && int.TryParse(name.AsSpan(5, 6), CultureInfo.InvariantCulture, out var idx))
                    values.Add(idx);
            });

        Assert.Equal([1, 3], sink);
    }

    /// <summary>
    /// Missing directories are ignored without failing the caller.
    /// </summary>
    [Fact]
    public void TryCollectFileValuesIgnoresMissingDirectory()
    {
        var sink = new List<int>();

        DirectoryKit.TryCollectFileValues(PathKit.Combine(_dir, "missing"), "item-*.dat", sink, static (_, values) => { values.Add(1); });

        Assert.Empty(sink);
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
