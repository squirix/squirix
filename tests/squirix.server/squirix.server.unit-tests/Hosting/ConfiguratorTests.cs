using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers the public server configuration loader.</summary>
public sealed class ConfiguratorTests : ServerUnitTestBase
{
    /// <summary>Ensures cluster settings can be loaded from a settings file path.</summary>
    [Fact]
    public async Task LoadFromFileReadsClusterSection()
    {
        using var dir = new TempDirectory("squirix-server-config");
        const string json = """{"Squirix":{"Cluster":{"ClusterId":"c1","NodeId":"node-a","Uri":"https://localhost:5001","VirtualNodes":128,"Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]}}}""";
        var path = NodePathKit.Combine(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var options = await Configurator.LoadFromFileAsync(path, DefaultCancellationToken);
        Assert.Equal("node-a", options.NodeId);
        Assert.Equal("c1", options.ClusterId);
    }

    /// <summary>Ensures invalid peer topology returns structured errors.</summary>
    [Fact]
    public async Task TryLoadFromFileReturnsErrorsForInvalidPeers()
    {
        using var dir = new TempDirectory("squirix-server-config-invalid");
        const string json = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-b","Uri":"https://localhost:5002"}]}}}""";
        var path = NodePathKit.Combine(dir, "invalid.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, _, error) = await Configurator.TryLoadFromFileAsync(path, DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("local NodeId", error, StringComparison.Ordinal);
    }

    /// <summary>Ensures TryValidate surfaces multiple validation failures.</summary>
    [Fact]
    public void OptionsValidateReturnsErrorsWithoutThrowing()
    {
        var options = new SquirixServerOptions { NodeId = string.Empty, VirtualNodes = 0 };
        var ok = options.TryValidate(out var errors);
        Assert.False(ok);
        Assert.True(errors.Count >= 2);
    }

    /// <summary>Ensures strict validation rejects invalid memory pressure thresholds.</summary>
    [Fact]
    public async Task TryValidateSettingsFileStrictRejectsInvalidMemoryPressure()
    {
        using var dir = new TempDirectory("squirix-server-config-strict");
        const string json = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001","Peers":[{"NodeId":"node-a","Uri":"https://localhost:5001"}]},"MemoryPressure":{"highPressureThresholdPercent":95,"criticalPressureThresholdPercent":80}}}""";
        var path = NodePathKit.Combine(dir, "strict.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, error) = await Configurator.TryValidateSettingsFileAsync(path, true, DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("HighPressureThresholdPercent", error, StringComparison.Ordinal);
    }

    /// <summary>Rejects settings paths that contain parent-directory segments.</summary>
    [Fact]
    public async Task TryLoadFromFileRejectsTraversalSettingsPath()
    {
        var (success, _, error) = await Configurator.TryLoadFromFileAsync("../Squirix.settings.json", DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("'.' or '..'", error, StringComparison.Ordinal);
    }

    /// <summary>Rejects command-line data directory overrides that contain parent-directory segments.</summary>
    [Fact]
    public void ApplyCommandLineOverridesRejectsTraversalDataDirectory()
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

        var ex = Assert.Throws<ArgumentException>(() => Configurator.ApplyCommandLineOverrides(options, null, "../data", true));
        Assert.Contains("'.' or '..'", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Canonicalizes a safe data directory override to an absolute path.</summary>
    [Fact]
    public void ApplyCommandLineOverridesCanonicalizesDataDirectory()
    {
        using var dir = new TempDirectory("squirix-server-config-datadir");
        var options = new SquirixServerOptions
        {
            NodeId = "node-a",
            Uri = new Uri("https://localhost:5001"),
            Peers =
            [
                new SquirixServerPeerOptions { NodeId = "node-a", Uri = new Uri("https://localhost:5001") },
            ],
        };

        Configurator.ApplyCommandLineOverrides(options, null, dir.Path, true);
        Assert.Equal(Path.GetFullPath(dir.Path), options.DataDirectory);
    }

    /// <summary>ApplyRuntimeDefaults canonicalizes an existing data directory.</summary>
    [Fact]
    public void ApplyRuntimeDefaultsCanonicalizesDataDirectory()
    {
        using var dir = new TempDirectory("squirix-server-config-runtime-datadir");
        var options = new SquirixServerOptions { DataDirectory = dir.Path };
        Configurator.ApplyRuntimeDefaults(options);
        Assert.Equal(Path.GetFullPath(dir.Path), options.DataDirectory);
    }

    /// <summary>Public path helpers reject traversal segments.</summary>
    [Fact]
    public void ResolveValidatedHelpersRejectTraversal()
    {
        _ = Assert.Throws<ArgumentException>(static () => Configurator.ResolveValidatedDataDirectory("../data"));
        _ = Assert.Throws<ArgumentException>(static () => Configurator.ResolveValidatedFilePath("../Squirix.settings.json"));
    }

    /// <summary>ResolveSettingsPath validates an explicit settings path.</summary>
    [Fact]
    public void ResolveSettingsPathCanonicalizesExplicitPath()
    {
        using var dir = new TempDirectory("squirix-server-config-settings-path");
        var path = Path.Join(dir.Path, "Squirix.settings.json");
        File.WriteAllText(path, "{}");
        var resolved = Configurator.ResolveSettingsPath(path);
        Assert.Equal(Path.GetFullPath(path), resolved);
    }

    /// <summary>TryLoadFromFile reports a clear error when the settings file is missing.</summary>
    [Fact]
    public async Task TryLoadFromFileReturnsErrorWhenFileMissing()
    {
        using var dir = new TempDirectory("squirix-server-config-missing");
        var (success, _, error) = await Configurator.TryLoadFromFileAsync(Path.Join(dir.Path, "missing.json"), DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("does not exist", error, StringComparison.OrdinalIgnoreCase);
    }
}
