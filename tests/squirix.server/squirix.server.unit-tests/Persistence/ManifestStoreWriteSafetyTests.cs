using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Manifest;
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
        var options = ManifestStoreTestSupport.CreateOptions(dir);
        using var store = new ManifestStore(options);
        await store.WriteAsync(new State { CurrentJournal = 1 }, DefaultCancellationToken);

        var first = PathKit.Combine(dir, ManifestStoreTestSupport.ManifestDataFileName(1));
        Assert.True(File.Exists(first));

        await store.WriteAsync(new State { CurrentJournal = 2 }, DefaultCancellationToken);

        var second = PathKit.Combine(dir, ManifestStoreTestSupport.ManifestDataFileName(2));
        Assert.True(File.Exists(second));
        Assert.Equal(2, await ManifestStoreTestSupport.ReadCurrentManifestIndexAsync(dir, DefaultCancellationToken));
    }
}
