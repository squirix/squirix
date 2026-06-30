using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Tests JSON merge and configuration binding for snapshot trigger settings.</summary>
public sealed class SnapshotSettingsBindingTests : UnitTestBase
{
    /// <summary>
    /// Verifies <see cref="UnifiedSettings.TryMergeSnapshotFromFileAsync" /> reads the <c>Snapshot</c> section.
    /// </summary>
    [Fact]
    public async Task UnifiedSettingsMergesSnapshotSectionFromFile()
    {
        using var dir = new TempDirectory("squirix-snapshot-settings-merge");
        const string json = """{"Squirix":{"Snapshot":{"snapshotInterval":"00:01:00","snapshotEveryNOps":42}}}""";
        var path = PathKit.Combine(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (found, merged) = await UnifiedSettings.TryMergeSnapshotFromSettingsFilePathAsync(path, new SnapshotTriggerOptions(), DefaultCancellationToken);
        Assert.True(found);
        Assert.Equal(TimeSpan.FromMinutes(1), merged.SnapshotInterval);
        Assert.Equal(42, merged.SnapshotEveryNOps);
        Assert.Equal(128L * 1024 * 1024, merged.SnapshotEveryNBytes);
    }

    /// <summary>Verifies strict settings validation includes a valid <c>Snapshot</c> section.</summary>
    [Fact]
    public async Task TryValidateSettingsFileStrictAcceptsValidSnapshotSection()
    {
        using var dir = new TempDirectory("squirix-snapshot-settings-strict");
        const string json =
            """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]},"Snapshot":{"SnapshotInterval":"00:01:00","SnapshotEveryNOps":42,"SnapshotEveryNBytes":1024,"MinGapBetweenSnapshots":"00:00:10"}}}""";
        var path = PathKit.Combine(dir, "strict.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, error) = await SquirixServerConfiguration.TryValidateSettingsFileAsync(path, true, DefaultCancellationToken);
        Assert.True(success, error);
    }
}
