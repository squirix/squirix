using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Durability;

/// <summary>
/// Durability behavior tests for manifest persistence and CURRENT pointer updates.
/// </summary>
public sealed class WindowsDurabilityTests : ServerUnitTestBase, IAsyncLifetime
{
    private string _dir = null!;

    /// <summary>
    /// Verifies that <see cref="ManifestStore" /> creates an initial manifest and updates the CURRENT pointer.
    /// </summary>
    [Fact]
    public void ManifestStoreCreatesCurrentPointerOnFirstWrite()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var store = new ManifestStore(options);

        store.Write(new Manifest { CurrentJournal = 1, NextSequence = 1 });
        var currentPath = PathKit.Combine(_dir, "man-current");
        Assert.True(File.Exists(currentPath));
        Assert.Equal("man-000001.msqx", File.ReadAllText(currentPath).Trim());
    }

    /// <summary>
    /// Verifies that first boot without a current pointer returns a default manifest.
    /// </summary>
    [Fact]
    public void ManifestStoreReturnsDefaultWhenCurrentPointerIsMissing()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var store = new ManifestStore(options);

        var manifest = store.ReadCurrentOrDefault();

        Assert.Equal(1, manifest.CurrentJournal);
        Assert.Equal(1UL, manifest.NextSequence);
    }

    /// <summary>
    /// Verifies that an empty current pointer is treated as storage corruption.
    /// </summary>
    [Fact]
    public void ManifestStoreThrowsWhenCurrentPointerIsEmpty()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var store = new ManifestStore(options);
        File.WriteAllText(PathKit.Combine(_dir, "man-current"), string.Empty);

        _ = Assert.Throws<InvalidDataException>(store.ReadCurrentOrDefault);
    }

    /// <summary>
    /// Verifies that a missing current pointer target is treated as storage corruption.
    /// </summary>
    [Fact]
    public void ManifestStoreThrowsWhenCurrentPointerTargetIsMissing()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var store = new ManifestStore(options);
        File.WriteAllText(PathKit.Combine(_dir, "man-current"), "man-000123.msqx");

        _ = Assert.Throws<FileNotFoundException>(store.ReadCurrentOrDefault);
    }

    /// <summary>
    /// Verifies that subsequent manifest writes update the CURRENT pointer to the new manifest file.
    /// </summary>
    [Fact]
    public void ManifestStoreUpdatesCurrentPointerOnRewrite()
    {
        var options = new PersistenceOptions { DataDir = _dir };
        var store = new ManifestStore(options);

        store.Write(new Manifest { CurrentJournal = 1, NextSequence = 1 });
        var currentPath = PathKit.Combine(_dir, "man-current");

        store.Write(new Manifest { CurrentJournal = 2, NextSequence = 10 });
        Assert.True(File.Exists(currentPath));
        Assert.Equal("man-000002.msqx", File.ReadAllText(currentPath).Trim());
    }

    /// <summary>
    /// Cleans up the temporary directory after the test.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> representing the asynchronous operation.</returns>
    public ValueTask DisposeAsync()
    {
        DirectoryKit.TryDeleteDirectory(_dir);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Creates a temporary directory for test storage.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> representing the asynchronous operation.</returns>
    public ValueTask InitializeAsync()
    {
        _dir = DirectoryKit.CreateTempDirectory("squirix");
        return ValueTask.CompletedTask;
    }
}
