using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Tests JSON merge and configuration binding for snapshot trigger settings.</summary>
[Immutable]
public sealed class SettingsBindingTests : IsolatedStorageTestBase
{
    /// <summary>Verifies strict settings validation includes a valid <c language="csharp">Snapshot</c> section.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ValidateFileAcceptsSnapshotSection(CancellationToken cancellationToken)
    {
        const string json =
            """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]},"Snapshot":{"SnapshotInterval":"00:01:00","SnapshotEveryNOps":42,"SnapshotEveryNBytes":1024,"MinGapBetweenSnapshots":"00:00:10"}}}""";
        var path = NodePathKit.Combine(Dir, "strict.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        var (success, _) = await Configurator.ValidateSettingsFileAsync(path, true, cancellationToken);
        _ = await Assert.That(success).IsTrue();
    }
}
