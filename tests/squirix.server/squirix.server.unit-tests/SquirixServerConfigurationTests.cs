using System;
using System.IO;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Covers the public server configuration loader.
/// </summary>
public sealed class SquirixServerConfigurationTests
{
    /// <summary>
    /// Ensures cluster settings can be loaded from a settings file path.
    /// </summary>
    [Fact]
    public void LoadFromFileReadsClusterSection()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-server-config");
        const string json = """
                            {
                              "Squirix": {
                                "Cluster": {
                                  "ClusterId": "c1",
                                  "NodeId": "node-a",
                                  "Url": "https://localhost:5001",
                                  "VirtualNodes": 128,
                                  "Peers": [
                                    { "NodeId": "node-a", "Url": "https://localhost:5001" }
                                  ]
                                }
                              }
                            }
                            """;
        var path = PathKit.Combine(dir, "Squirix.settings.json");
        File.WriteAllText(path, json);

        try
        {
            var options = SquirixServerConfiguration.LoadFromFile(path);
            Assert.Equal("node-a", options.NodeId);
            Assert.Equal("c1", options.ClusterId);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures invalid peer topology returns structured errors.
    /// </summary>
    [Fact]
    public void TryLoadFromFileReturnsErrorsForInvalidPeers()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-server-config-invalid");
        const string json = """
                            {
                              "Squirix": {
                                "Cluster": {
                                  "NodeId": "node-a",
                                  "Url": "https://localhost:5001",
                                  "Peers": [
                                    { "NodeId": "node-b", "Url": "https://localhost:5002" }
                                  ]
                                }
                              }
                            }
                            """;
        var path = PathKit.Combine(dir, "invalid.json");
        File.WriteAllText(path, json);

        try
        {
            var ok = SquirixServerConfiguration.TryLoadFromFile(path, out _, out var error);
            Assert.False(ok);
            Assert.Contains("local NodeId", error, StringComparison.Ordinal);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures TryValidate surfaces multiple validation failures.
    /// </summary>
    [Fact]
    public void TryValidateReturnsErrorsWithoutThrowing()
    {
        var options = new SquirixServerOptions { NodeId = string.Empty, VirtualNodes = 0 };
        var ok = options.TryValidate(out var errors);
        Assert.False(ok);
        Assert.True(errors.Count >= 2);
    }

    /// <summary>
    /// Ensures strict validation rejects invalid memory pressure thresholds.
    /// </summary>
    [Fact]
    public void TryValidateSettingsFileStrictRejectsInvalidMemoryPressure()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-server-config-strict");
        const string json = """
                            {
                              "Squirix": {
                                "Cluster": {
                                  "NodeId": "node-a",
                                  "Url": "https://localhost:5001",
                                  "Peers": [
                                    { "NodeId": "node-a", "Url": "https://localhost:5001" }
                                  ]
                                },
                                "MemoryPressure": {
                                  "HighPressureThresholdPercent": 95,
                                  "CriticalPressureThresholdPercent": 80
                                }
                              }
                            }
                            """;
        var path = PathKit.Combine(dir, "strict.json");
        File.WriteAllText(path, json);

        try
        {
            var ok = SquirixServerConfiguration.TryValidateSettingsFile(path, true, out var error);
            Assert.False(ok);
            Assert.Contains("HighPressureThresholdPercent", error, StringComparison.Ordinal);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
