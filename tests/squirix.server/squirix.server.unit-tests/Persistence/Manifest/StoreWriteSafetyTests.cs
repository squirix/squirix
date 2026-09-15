using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Safety tests for <see cref="Ledger.WriteAsync" /> when <c language="csharp">CURRENT</c> or on-disk manifests are corrupt.</summary>
[Immutable]
public sealed class StoreWriteSafetyTests : IsolatedStorageTestBase
{
    /// <summary>Verifies monotonic manifest writes advance the index when <c language="csharp">CURRENT</c> is valid.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteAdvancesIndexForValidCurrent(CancellationToken cancellationToken)
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options);
        await store.WriteAsync(new State { CurrentJournal = 1 }, cancellationToken);

        var first = NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(1));
        _ = await Assert.That(File.Exists(first)).IsTrue();

        await store.WriteAsync(new State { CurrentJournal = 2 }, cancellationToken);

        var second = NodePathKit.Combine(Dir, StoreTestSupport.ManifestDataFileName(2));
        _ = await Assert.That(File.Exists(second)).IsTrue();
        _ = await Assert.That(await StoreTestSupport.ReadCurrentManifestIndexAsync(Dir, cancellationToken)).IsEqualTo(2);
    }
}
