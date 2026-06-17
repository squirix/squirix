using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers the public server configuration loader.</summary>
public sealed class SquirixServerConfigurationTests : UnitTestBase
{
    /// <summary>Ensures cluster settings can be loaded from a settings file path.</summary>
    [Fact]
    public async Task LoadFromFileReadsClusterSection()
    {
        using var dir = new TempDirectory("squirix-server-config");
        const string json = """{"Squirix":{"Cluster":{"ClusterId":"c1","NodeId":"node-a","Url":"https://localhost:5001","VirtualNodes":128,"Peers":[{"NodeId":"node-a","Url":"https://localhost:5001"}]}}}""";
        var path = PathKit.Combine(dir, "Squirix.settings.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var options = await SquirixServerConfiguration.LoadFromFileAsync(path, DefaultCancellationToken);
        Assert.Equal("node-a", options.NodeId);
        Assert.Equal("c1", options.ClusterId);
    }

    /// <summary>Ensures invalid peer topology returns structured errors.</summary>
    [Fact]
    public async Task TryLoadFromFileReturnsErrorsForInvalidPeers()
    {
        using var dir = new TempDirectory("squirix-server-config-invalid");
        const string json = """{"Squirix":{"Cluster":{"NodeId":"node-a","Url":"https://localhost:5001","Peers":[{"NodeId":"node-b","Url":"https://localhost:5002"}]}}}""";
        var path = PathKit.Combine(dir, "invalid.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, _, error) = await SquirixServerConfiguration.TryLoadFromFileAsync(path, DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("local NodeId", error, StringComparison.Ordinal);
    }

    /// <summary>Ensures TryValidate surfaces multiple validation failures.</summary>
    [Fact]
    public void TryValidateReturnsErrorsWithoutThrowing()
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
        const string json = """{"Squirix":{"Cluster":{"NodeId":"node-a","Url":"https://localhost:5001","Peers":[{"NodeId":"node-a","Url":"https://localhost:5001"}]},"MemoryPressure":{"HighPressureThresholdPercent":95,"CriticalPressureThresholdPercent":80}}}""";
        var path = PathKit.Combine(dir, "strict.json");
        await File.WriteAllTextAsync(path, json, DefaultCancellationToken);
        var (success, error) = await SquirixServerConfiguration.TryValidateSettingsFileAsync(path, true, DefaultCancellationToken);
        Assert.False(success);
        Assert.Contains("HighPressureThresholdPercent", error, StringComparison.Ordinal);
    }
}
