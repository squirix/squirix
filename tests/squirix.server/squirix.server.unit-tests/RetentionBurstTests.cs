using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies async retention does not delete the active manifest during publish bursts.</summary>
public sealed class RetentionBurstTests : ServerUnitTestBase, IAsyncLifetime
{
    private TempDirectory? _dir;

    private TempDirectory Dir => _dir ?? throw new InvalidOperationException("Test directory is not initialized.");

    /// <summary>Rapid publishes retain the latest manifest file and pointer.</summary>
    [Fact]
    public async Task RapidPublishBurstKeepsCurrentManifest()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir.Path,
            ManifestRetentionCount = 2,
        };
        using var store = new ManifestStore(options);

        for (var i = 1; i <= 20; i++)
            store.PublishRollBlocking(i, Convert.ToUInt64(i));

        var manifestPath = NodePathKit.Combine(Dir.Path, StoreTestSupport.ManifestDataFileName(20));
        await StoreTestSupport.WaitUntilAsync(manifestPath, static path => File.Exists(path), TimeSpan.FromSeconds(5), DefaultCancellationToken);

        var currentPath = NodePathKit.Combine(Dir.Path, StoreTestSupport.ManifestCurrentPointer);
        Assert.Equal(20, Pointer.Read(await File.ReadAllBytesAsync(currentPath, DefaultCancellationToken)));
        Assert.True(File.Exists(NodePathKit.Combine(Dir.Path, StoreTestSupport.ManifestDataFileName(20))));
    }

    /// <summary>Disposes the temporary directory after the test class finishes.</summary>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Creates a temporary directory for test storage.</summary>
    public ValueTask InitializeAsync()
    {
        _dir = new TempDirectory("manifest-burst");
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _dir?.Dispose();

        base.Dispose(disposing);
    }
}
