using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Safety tests for <see cref="ManifestStore.WriteAsync" /> when <c>CURRENT</c> or on-disk manifests are corrupt.
/// </summary>
public sealed class ManifestStoreWriteSafetyTests : UnitTestBase
{
    /// <summary>
    /// Verifies monotonic manifest writes advance the index when <c>CURRENT</c> is valid.
    /// </summary>
    [Fact]
    public async Task WriteAdvancesManifestIndexWhenCurrentIsValid()
    {
        using var dir = new TempDirectory("manifest-store-monotonic");
        var options = new PersistenceOptions { DataDir = dir };
        using var store = new ManifestStore(options);
        await store.WriteAsync(new Manifest { CurrentJournal = 1 }, DefaultCancellationToken);

        var first = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}");
        Assert.True(File.Exists(first));

        await store.WriteAsync(new Manifest { CurrentJournal = 2 }, DefaultCancellationToken);

        var second = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000002{StorageFileExtensions.Manifest}");
        Assert.True(File.Exists(second));
        var currentPointer = await File.ReadAllTextAsync(PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}current"), DefaultCancellationToken);
        Assert.Contains("000002", currentPointer, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies corrupt <c>CURRENT</c> does not overwrite an existing manifest file.
    /// </summary>
    [Fact]
    public async Task WriteThrowsWhenCurrentIsCorruptAndManifestAlreadyExists()
    {
        using var dir = new TempDirectory("manifest-store-corrupt-current");
        var options = new PersistenceOptions { DataDir = dir };
        using var store = new ManifestStore(options);
        var existingPath = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}");
        var existingBytes = """{"schemaVersion":1,"currentJournal":1}"""u8.ToArray();
        await File.WriteAllBytesAsync(existingPath, existingBytes, DefaultCancellationToken);
        await File.WriteAllTextAsync(PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}current"), "not-a-manifest-name", DefaultCancellationToken);

        var ex = await Assert.ThrowsAsync<InvalidDataException>(() => store.WriteAsync(new Manifest { CurrentJournal = 2 }, DefaultCancellationToken));
        Assert.Contains("current pointer", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(File.Exists(existingPath));
        Assert.Equal(existingBytes, await File.ReadAllBytesAsync(existingPath, DefaultCancellationToken));
    }
}
