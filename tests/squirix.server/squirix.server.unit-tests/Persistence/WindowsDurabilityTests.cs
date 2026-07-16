using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Durability behavior tests for manifest persistence and CURRENT pointer updates.</summary>
public sealed class WindowsDurabilityTests : ServerUnitTestBase, IAsyncLifetime
{
    private TempDirectory? _dir;

    private TempDirectory Dir => _dir ?? throw new InvalidOperationException("Test directory is not initialized.");

    /// <summary>
    /// Verifies that <see cref="ManifestStore" /> creates an initial manifest and updates the CURRENT pointer.
    /// </summary>
    [Fact]
    public async Task ManifestStoreCreatesCurrentPointerOnFirstWrite()
    {
        var options = ManifestStoreTestSupport.CreateOptions(Dir);
        using var store = new ManifestStore(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);
        var currentPath = PathKit.Combine(Dir, "man-current");
        Assert.True(File.Exists(currentPath));
        Assert.Equal(1, await ManifestStoreTestSupport.ReadCurrentManifestIndexAsync(Dir, DefaultCancellationToken));
    }

    /// <summary>Verifies that first boot without a current pointer returns a default manifest.</summary>
    [Fact]
    public async Task ManifestStoreReturnsDefaultCurrentPointerIsMissing()
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new ManifestStore(options);

        var manifest = await store.ReadCurrentOrDefaultAsync(DefaultCancellationToken);

        Assert.Equal(1, manifest.CurrentJournal);
        Assert.Equal(1UL, manifest.NextSequence);
    }

    /// <summary>Verifies that a missing current pointer target is treated as storage corruption.</summary>
    [Fact]
    public async Task ManifestStoreThrowsCurrentPointerTargetIsMissing()
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new ManifestStore(options);
        WriteCurrentPointer(Dir, 123);

        _ = await NodeAsyncAssert.ThrowsAsync<FileNotFoundException>(store.ReadCurrentOrDefaultAsync(DefaultCancellationToken));
    }

    /// <summary>Verifies that an empty current pointer is treated as storage corruption.</summary>
    [Fact]
    public async Task ManifestStoreThrowsWhenCurrentPointerIsEmpty()
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new ManifestStore(options);
        await File.WriteAllBytesAsync(PathKit.Combine(Dir, "man-current"), ReadOnlyMemory<byte>.Empty, DefaultCancellationToken);

        _ = await Assert.ThrowsAsync<InvalidDataException>(() => store.ReadCurrentOrDefaultAsync(DefaultCancellationToken));
    }

    /// <summary>Verifies that a missing current pointer target is treated as storage corruption.</summary>
    [Fact]
    public async Task ManifestStoreThrowsWhenCurrentPointerTargetIsMissing()
    {
        var options = new PersistenceOptions { DataDir = Dir };
        using var store = new ManifestStore(options);
        WriteCurrentPointer(Dir, 123);

        _ = await Assert.ThrowsAsync<FileNotFoundException>(() => store.ReadCurrentOrDefaultAsync(DefaultCancellationToken));
    }

    /// <summary>Verifies that subsequent manifest writes update the CURRENT pointer to the new manifest file.</summary>
    [Fact]
    public async Task ManifestStoreUpdatesCurrentPointerOnRewrite()
    {
        var options = ManifestStoreTestSupport.CreateOptions(Dir);
        using var store = new ManifestStore(options);

        await store.WriteAsync(new State { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);
        await store.WriteAsync(new State { CurrentJournal = 2, NextSequence = 10 }, DefaultCancellationToken);
        Assert.Equal(2, await ManifestStoreTestSupport.ReadCurrentManifestIndexAsync(Dir, DefaultCancellationToken));
    }

    /// <summary>Creates a temporary directory for test storage.</summary>
    public ValueTask InitializeAsync()
    {
        _dir = new TempDirectory("squirix");
        return ValueTask.CompletedTask;
    }

    /// <summary>Cleans up the temporary directory after the test.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _dir?.Dispose();

        base.Dispose(disposing);
    }

    private static void WriteCurrentPointer(TempDirectory dir, int manifestIndex)
    {
        Span<byte> pointerBuffer = stackalloc byte[Pointer.Size];
        Pointer.Write(pointerBuffer, manifestIndex);
        File.WriteAllBytes(PathKit.Combine(dir, "man-current"), pointerBuffer);
    }
}
