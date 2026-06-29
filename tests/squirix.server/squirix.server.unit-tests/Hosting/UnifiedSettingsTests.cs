using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Tests for unified JSON settings discovery and merge helpers.</summary>
public sealed class UnifiedSettingsTests : UnitTestBase
{
    /// <summary>
    /// Verifies cluster configuration can be loaded from an explicit <c>Squirix.settings.json</c> path without mutating
    /// <see cref="Environment.CurrentDirectory" /> (safe for parallel test runs).
    /// </summary>
    [Fact]
    public async Task TryLoadClusterConfigFromSettingsFilePathParsesClusterSectionTest()
    {
        using var dir = new TempDirectory("squirix-unified-cluster-json");
        const string json = """{"Squirix":{"Cluster":{"NodeId":"alpha","Uri":"https://127.0.0.1:60443","Peers":[{"NodeId":"alpha","Uri":"https://127.0.0.1:60443"}]}}}""";
        var settingsPath = PathKit.Combine(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(settingsPath, json, DefaultCancellationToken);
        var (found, cfg) = await UnifiedSettings.TryLoadClusterConfigFromSettingsFilePathAsync(settingsPath, DefaultCancellationToken);
        Assert.True(found);
        Assert.NotNull(cfg);
        Assert.Equal("alpha", cfg.NodeId);
        _ = Assert.Single(cfg.Peers);
    }

    /// <summary>Verifies memory pressure JSON merges onto caller-supplied baselines.</summary>
    [Fact]
    public async Task TryMergeMemoryPressureFromSettingsFilePathMergesSection()
    {
        using var dir = new TempDirectory("squirix-unified-memory-json");
        var path = PathKit.Combine(dir, "mp.json");
        const string json = """{"Squirix":{"MemoryPressure":{"MaxEstimatedCacheBytes":7777}}}""";
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var baseline = new UnresolvedMemoryPressureOptions();
        var (found, merged) = await UnifiedSettings.TryMergeMemoryPressureFromSettingsFilePathAsync(path, baseline, DefaultCancellationToken);
        Assert.True(found);
        Assert.Equal(7777, merged.MaxEstimatedCacheBytes);
    }
}
