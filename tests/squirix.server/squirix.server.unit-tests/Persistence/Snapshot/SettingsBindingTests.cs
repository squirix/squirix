using System.IO;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Tests JSON merge and configuration binding for snapshot trigger settings.</summary>
[Immutable]
public sealed class SettingsBindingTests : ServerUnitTestBase
{
    /// <summary>Verifies strict settings validation includes a valid <c>Snapshot</c> section.</summary>
    [Fact]
    public async Task TryValidateSettingsFileAcceptsValidSnapshotSection()
    {
        using var dir = new TempDirectory("squirix-snapshot-settings-strict");
        const string json =
            """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]},"Snapshot":{"SnapshotInterval":"00:01:00","SnapshotEveryNOps":42,"SnapshotEveryNBytes":1024,"MinGapBetweenSnapshots":"00:00:10"}}}""";
        var path = NodePathKit.Combine(dir, "strict.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, _) = await Configurator.TryValidateSettingsFileAsync(path, true, DefaultCancellationToken);
        Assert.True(success);
    }
}
