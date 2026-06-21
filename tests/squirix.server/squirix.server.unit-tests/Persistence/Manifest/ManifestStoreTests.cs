using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Integration tests for the manifest store.</summary>
public sealed class ManifestStoreTests : UnitTestBase, IAsyncLifetime
{
    private TempDirectory? _dir;

    private TempDirectory Dir => _dir ?? throw new InvalidOperationException("Test directory is not initialized.");

    /// <summary>Verifies the first write creates a current pointer and numbered manifest file.</summary>
    [Fact]
    public async Task WriteAsyncCreatesCurrentPointerAndManifestFile()
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        using var store = new ManifestStore(options);

        await store.WriteAsync(new Storage.Manifest.ManifestState { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);

        var currentPath = PathKit.Combine(Dir.Path, "man-current");
        var pointerBytes = await File.ReadAllBytesAsync(currentPath, DefaultCancellationToken);
        Assert.Equal(12, pointerBytes.Length);
        Assert.Equal(1, ManifestPointer.Read(pointerBytes));

        var manifestPath = PathKit.Combine(Dir.Path, "man-000001.bmqx");
        Assert.True(File.Exists(manifestPath));
        var manifest = ManifestCodec.Decode(await File.ReadAllBytesAsync(manifestPath, DefaultCancellationToken));
        Assert.Equal(1, manifest.CurrentJournal);
        Assert.Equal(1UL, manifest.NextSequence);
    }

    /// <summary>Verifies sequential roll publishes advance the current pointer while a persistent handle stays open.</summary>
    [Fact]
    public async Task PublishRollBlockingIncrementsIndexWithoutDiskRead()
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        using var store = new ManifestStore(options);

        store.PublishRollBlocking(1, 1);
        store.PublishRollBlocking(2, 2);

        var currentPath = PathKit.Combine(Dir.Path, "man-current");
        Assert.Equal(2, ManifestPointer.Read(await File.ReadAllBytesAsync(currentPath, DefaultCancellationToken)));
    }

    /// <summary>Verifies CURRENT is updated in place without leaving a temp pointer file.</summary>
    [Fact]
    public async Task WriteAsyncUpdatesCurrentPointerInPlaceWithoutTmpFile()
    {
        var options = new PersistenceOptions { DataDir = Dir.Path };
        using var store = new ManifestStore(options);

        await store.WriteAsync(new Storage.Manifest.ManifestState { CurrentJournal = 1, NextSequence = 1 }, DefaultCancellationToken);

        Assert.False(File.Exists(PathKit.Combine(Dir.Path, "man-current.tmp")));
        Assert.Equal(12, (await File.ReadAllBytesAsync(PathKit.Combine(Dir.Path, "man-current"), DefaultCancellationToken)).Length);
    }

    /// <summary>Disposes the temporary directory after the test class finishes.</summary>
    public ValueTask DisposeAsync()
    {
        _dir?.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Creates a temporary directory for test storage.</summary>
    public ValueTask InitializeAsync()
    {
        _dir = new TempDirectory("manifest");
        return ValueTask.CompletedTask;
    }
}
