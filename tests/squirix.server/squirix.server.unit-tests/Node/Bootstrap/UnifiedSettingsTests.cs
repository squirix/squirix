using System;
using System.IO;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Bootstrap;

/// <summary>
/// Tests for unified JSON settings discovery and merge helpers.
/// </summary>
public sealed class UnifiedSettingsTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies cluster configuration can be loaded from an explicit <c>Squirix.settings.json</c> path without mutating
    /// <see cref="Environment.CurrentDirectory" /> (safe for parallel test runs).
    /// </summary>
    [Fact]
    public void TryLoadClusterConfigFromSettingsFilePathParsesClusterSectionTest()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-unified-cluster-json");
        const string json = """
                            {
                              "Squirix": {
                                "Cluster": {
                                  "NodeId": "alpha",
                                  "Url": "https://127.0.0.1:60443",
                                  "Peers": [
                                    { "NodeId": "alpha", "Url": "https://127.0.0.1:60443" }
                                  ]
                                }
                              }
                            }
                            """;
        var settingsPath = PathKit.Combine(dir, "Squirix.settings.json");
        File.WriteAllText(settingsPath, json);

        try
        {
            Assert.True(UnifiedSettings.TryLoadClusterConfigFromSettingsFilePath(settingsPath, out var cfg));
            Assert.Equal("alpha", cfg.NodeId);
            _ = Assert.Single(cfg.Peers);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Verifies memory pressure JSON merges onto caller-supplied baselines.
    /// </summary>
    [Fact]
    public void TryMergeMemoryPressureFromSettingsFilePathMergesSection()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-unified-memory-json");
        var path = PathKit.Combine(dir, "mp.json");
        const string json = """
                            {
                              "Squirix": {
                                "MemoryPressure": {
                                  "Enabled": true,
                                  "MaxEstimatedCacheBytes": 7777
                                }
                              }
                            }
                            """;
        File.WriteAllText(path, json);
        var baseline = new MemoryPressureOptions { Enabled = false, MaxEstimatedCacheBytes = null };
        try
        {
            Assert.True(UnifiedSettings.TryMergeMemoryPressureFromSettingsFilePath(path, baseline, out var merged));
            Assert.True(merged.Enabled);
            Assert.Equal(7777, merged.MaxEstimatedCacheBytes);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
