using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers the public server configuration loader.</summary>
[Immutable]
public sealed class ConfiguratorTests : IsolatedStorageTestBase
{
    /// <summary>Canonicalizes a safe data directory override to an absolute path.</summary>
    [Fact]
    public void CommandLineCanonicalizesDataDir()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };

        Configurator.ApplyCommandLineOverrides(options, null, Dir.Path, true);
        Assert.Equal(Path.GetFullPath(Dir.Path), options.DataDirectory);
    }

    /// <summary>Rejects command-line data directory overrides that contain parent-directory segments.</summary>
    [Fact]
    public void CommandLineOverridesTraversalDataDir()
    {
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };

        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(options, static value => Configurator.ApplyCommandLineOverrides(value, null, "../data", true));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>ApplyRuntimeDefaults canonicalizes an existing data directory.</summary>
    [Fact]
    public void RuntimeDefaultsCanonicalizeDataDir()
    {
        var options = new SquirixServerOptions { DataDirectory = Dir.Path };
        Configurator.ApplyRuntimeDefaults(options);
        Assert.Equal(Path.GetFullPath(Dir.Path), options.DataDirectory);
    }

    /// <summary>CopyOptions preserves replica placement fields.</summary>
    [Fact]
    public void CopyOptionsCopiesReplicaSettings()
    {
        var source = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            ReplicaCount = 3,
            ConfigurationGeneration = 9,
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };
        var target = new SquirixServerOptions();
        Configurator.CopyOptions(source, target);
        Assert.Equal(3, target.ReplicaCount);
        Assert.Equal(9u, target.ConfigurationGeneration);
    }

    /// <summary>Ensures cluster settings can be loaded from a settings file path.</summary>
    [Fact]
    public async Task LoadFromFileReadsClusterSection()
    {
        const string json =
            """{"Squirix":{"Cluster":{"ClusterId":"c1","NodeId":"node-a","Uri":"https://localhost:5001","VirtualNodes":128,"Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]}}}""";
        var path = NodePathKit.Combine(Dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var options = await Configurator.LoadFromFileAsync(path, DefaultCancellationToken);
        Assert.Equal("node-a", options.NodeId);
        Assert.Equal("c1", options.ClusterId);
    }

    /// <summary>Ensures TryValidate surfaces multiple validation failures.</summary>
    [Fact]
    public void ValidateReturnsErrorsWithoutThrowing()
    {
        var options = new SquirixServerOptions { NodeId = string.Empty, VirtualNodes = 0 };
        var ok = options.TryValidate(out var errors);
        Assert.False(ok);
        Assert.True(errors.Count >= 2);
    }

    /// <summary>ResolveSettingsPath validates an explicit settings path.</summary>
    [Fact]
    public void SettingsPathCanonicalizesExplicitInput()
    {
        var path = Path.Join(Dir.Path, "Squirix.settings.json");
        File.WriteAllText(path, "{}");
        var resolved = Configurator.ResolveSettingsPath(path);
        Assert.Equal(Path.GetFullPath(path), resolved);
    }

    /// <summary>Public path helpers reject traversal segments.</summary>
    [Fact]
    public void ResolveValidatedHelpersRejectTraversal()
    {
        _ = NodeExceptionAssert.For<ArgumentException>().Throws("../data", static value => Configurator.ResolveValidatedDataDirectory(value));
        _ = NodeExceptionAssert.For<ArgumentException>().Throws("../Squirix.settings.json", static value => Configurator.ResolveValidatedFilePath(value));
    }

    /// <summary>Rejects settings paths that contain parent-directory segments.</summary>
    [Fact]
    public async Task LoadFromFileRejectsTraversalPath()
    {
        var (success, _, error) = await Configurator.TryLoadFromFileAsync("../Squirix.settings.json", DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("'.' or '..'", error, StringComparison.Ordinal);
    }

    /// <summary>TryLoadFromFile reports a clear error when the settings file is missing.</summary>
    [Fact]
    public async Task LoadFromFileErrorsWhenFileMissing()
    {
        var (success, _, error) = await Configurator.TryLoadFromFileAsync(Path.Join(Dir.Path, "missing.json"), DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("does not exist", error, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Ensures invalid peer topology returns structured errors.</summary>
    [Fact]
    public async Task LoadFromFileErrorsForInvalidPeers()
    {
        const string json = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-b","Uri":"https://localhost:5002"}]}}}""";
        var path = NodePathKit.Combine(Dir, "invalid.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, _, error) = await Configurator.TryLoadFromFileAsync(path, DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("local NodeId", error, StringComparison.Ordinal);
    }

    /// <summary>Ensures strict validation rejects invalid memory pressure thresholds.</summary>
    [Fact]
    public async Task ValidateFileFlagsInvalidMemoryPressure()
    {
        const string json =
            """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]},"MemoryPressure":{"highPressureThresholdPercent":95,"criticalPressureThresholdPercent":80}}}""";
        var path = NodePathKit.Combine(Dir, "strict.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, error) = await Configurator.TryValidateSettingsFileAsync(path, true, DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("HighPressureThresholdPercent", error, StringComparison.Ordinal);
    }
}
