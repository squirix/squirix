using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies the RESP settings loaded from standalone configuration files.</summary>
[Immutable]
public sealed class RespConfigurationTests : IsolatedStorageTestBase
{
    /// <summary>Omitted RESP settings enable the listener by default and explicit settings can disable it.</summary>
    /// <param name="respSection">The optional RESP section to append to the settings.</param>
    /// <param name="enabled">Whether the loaded settings enable RESP.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("", true)]
    [Arguments(""","Resp":{}""", true)]
    [Arguments(""","Resp":{"Enabled":false}""", false)]
    public async Task FileRetainsRespEndpointDefaults(string respSection, bool enabled, CancellationToken cancellationToken)
    {
        const string cluster = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001"}""";
        var json = cluster + respSection + "}}";
        var path = PathKit.Combine(Dir, "resp-defaults.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        var options = await Configurator.LoadAsync(path, cancellationToken);
        _ = await Assert.That(options.Resp.Enabled).IsEqualTo(enabled);
        _ = await Assert.That(options.Resp.BindAddress).IsEqualTo("127.0.0.1");
        _ = await Assert.That(options.Resp.Port).IsEqualTo(6379);
    }

    /// <summary>Reads RESP settings from discovered appsettings without cluster configuration.</summary>
    /// <param name="enabled">Whether the file enables RESP.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task DiscoveryLoadsRespWithoutCluster(bool enabled, CancellationToken cancellationToken)
    {
        var path = PathKit.Combine(Dir, "appsettings.json");
        var enabledJson = enabled ? "true" : "false";
        var json = $$$$"""{"Squirix":{"Resp":{"Enabled":{{{{enabledJson}}}},"Port":6385}}}""";
        await File.WriteAllTextAsync(path, json, cancellationToken);

        var options = await Configurator.LoadOrCreateDefaultAsync(path, cancellationToken);

        _ = await Assert.That(options.SettingsFilePath).IsEqualTo(path);
        _ = await Assert.That(options.Resp.Enabled).IsEqualTo(enabled);
        _ = await Assert.That(options.Resp.Port).IsEqualTo(6385);
    }

    /// <summary>Explicit file validation checks the RESP port in the file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FileValidationRejectsInvalidRespPort(CancellationToken cancellationToken)
    {
        const string json = """{"Squirix":{"Cluster":{"NodeId":"node-a","Uri":"https://localhost:5001"},"Resp":{"Enabled":true,"Port":-1}}}""";
        var path = PathKit.Combine(Dir, "invalid-resp.json");
        await File.WriteAllTextAsync(path, json, cancellationToken);
        var (success, error) = await Configurator.ValidateSettingsFileAsync(path, false, cancellationToken);
        _ = await Assert.That(success).IsFalse();
        _ = await Assert.That(error).Contains("RESP port", StringComparison.Ordinal);
    }
}
