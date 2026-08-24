using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>
/// Safety tests for <see cref="Ledger.WriteAsync" /> when <c>CURRENT</c> or on-disk manifests are corrupt.
/// </summary>
[Immutable]
public sealed class StoreWriteSafetyTests : IsolatedStorageTestBase
{
    /// <summary>
    /// Verifies monotonic manifest writes advance the index when <c>CURRENT</c> is valid.
    /// </summary>
    [Fact]
    public async Task WriteAdvancesIndexForValidCurrent()
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options);
        await store.WriteAsync(new State { CurrentJournal = 1 }, DefaultCancellationToken);

        var first = NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(1));
        Assert.True(File.Exists(first));

        await store.WriteAsync(new State { CurrentJournal = 2 }, DefaultCancellationToken);

        var second = NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(2));
        Assert.True(File.Exists(second));
        Assert.Equal(2, await StoreTestSupport.ReadCurrentManifestIndexAsync(Dir, DefaultCancellationToken));
    }
}
