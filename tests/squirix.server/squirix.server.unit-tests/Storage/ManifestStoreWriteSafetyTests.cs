using System;
using System.IO;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Safety tests for <see cref="ManifestStore.Write" /> when <c>CURRENT</c> or on-disk manifests are corrupt.
/// </summary>
public sealed class ManifestStoreWriteSafetyTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies monotonic manifest writes advance the index when <c>CURRENT</c> is valid.
    /// </summary>
    [Fact]
    public void WriteAdvancesManifestIndexWhenCurrentIsValid()
    {
        var dir = DirectoryKit.CreateTempDirectory("manifest-store-monotonic");
        try
        {
            var options = new PersistenceOptions { DataDir = dir };
            var store = new ManifestStore(options);
            store.Write(new Manifest { CurrentJournal = 1 });

            var first = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}");
            Assert.True(File.Exists(first));

            store.Write(new Manifest { CurrentJournal = 2 });

            var second = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000002{StorageFileExtensions.Manifest}");
            Assert.True(File.Exists(second));
            Assert.Contains("000002", File.ReadAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}current")), StringComparison.Ordinal);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Verifies corrupt <c>CURRENT</c> does not overwrite an existing manifest file.
    /// </summary>
    [Fact]
    public void WriteThrowsWhenCurrentIsCorruptAndManifestAlreadyExists()
    {
        var dir = DirectoryKit.CreateTempDirectory("manifest-store-corrupt-current");
        try
        {
            var options = new PersistenceOptions { DataDir = dir };
            var store = new ManifestStore(options);
            var existingPath = PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}000001{StorageFileExtensions.Manifest}");
            var existingBytes = """{"schemaVersion":1,"currentJournal":1}"""u8.ToArray();
            File.WriteAllBytes(existingPath, existingBytes);
            File.WriteAllText(PathKit.Combine(dir, $"{StorageFilePrefixes.Manifest}current"), "not-a-manifest-name");

            var ex = Assert.Throws<InvalidDataException>(() => store.Write(new Manifest { CurrentJournal = 2 }));
            Assert.Contains("current pointer", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.True(File.Exists(existingPath));
            Assert.Equal(existingBytes, File.ReadAllBytes(existingPath));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
