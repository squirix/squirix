using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.BinaryManifest;

/// <summary>Verifies JSON-to-binary migration writes SQMC pointer and first bmqx file.</summary>
public sealed class ManifestBackendMigratorTests : UnitTestBase
{
    /// <summary>Migrates an existing JSON manifest directory to binary format.</summary>
    [Fact]
    public async Task MigrateJsonToBinaryWritesBinaryPointerAndManifest()
    {
        using var dir = new TempDirectory("manifest-migrate");
        var jsonOptions = new PersistenceOptions { DataDir = dir.Path, ManifestBackend = ManifestBackend.Json };
        using (var jsonStore = new ManifestStore(jsonOptions))
        {
            await jsonStore.WriteAsync(new Storage.Manifest.ManifestState { CurrentJournal = 3, NextSequence = 9 }, DefaultCancellationToken);
        }

        await Storage.Manifest.ManifestBackendMigrator.MigrateJsonToBinaryAsync(jsonOptions with { ManifestBackend = ManifestBackend.Binary }, DefaultCancellationToken);

        var pointerBytes = await File.ReadAllBytesAsync(PathKit.Combine(dir.Path, "man-current"), DefaultCancellationToken);
        Assert.Equal(1, BinaryManifestPointer.Read(pointerBytes));
        Assert.True(File.Exists(PathKit.Combine(dir.Path, "man-000001.bmqx")));
    }
}
