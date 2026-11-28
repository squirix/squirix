using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Verifies packaged host defaults and normal user-configuration precedence in a separate process.</summary>
[Property("Suite", "Smoke")]
public sealed class HostLoggingTests
{
    private const int DefaultConfig = 0;
    private const int FileConfig = 1;
    private const int EnvironmentFileConfig = 2;
    private const int EnvironmentConfig = 3;
    private const int EnvironmentOverFileConfig = 4;

    /// <summary>Verifies defaults work outside the binary directory and user settings retain higher priority.</summary>
    /// <param name="configuration">The user configuration to apply.</param>
    /// <param name="requestInfo">Whether informational framework request messages should be emitted.</param>
    /// <param name="lifetimeInfo">Whether ordinary host information messages should be emitted.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(DefaultConfig, false, false)]
    [Arguments(FileConfig, true, true)]
    [Arguments(EnvironmentFileConfig, true, false)]
    [Arguments(EnvironmentConfig, true, false)]
    [Arguments(EnvironmentOverFileConfig, false, false)]
    [Timeout(60_000)]
    public async Task LoggingDefaultsAllowOverrides(int configuration, bool requestInfo, bool lifetimeInfo, CancellationToken cancellationToken)
    {
        var server = await ServerProcess.StartAsync(false, cancellationToken, (start, token) => ConfigureAsync(start, configuration, token));
        try
        {
            using var response = await server.HttpClient.GetAsync(new Uri(server.ManagementUri, "/health/live"), cancellationToken);
            _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
        }
        finally
        {
            await server.DisposeAsync();
        }

        var output = await server.ReadOutputAsync();
        _ = await Assert.That(output.Contains("Management request", StringComparison.Ordinal)).IsEqualTo(requestInfo);
        _ = await Assert.That(output.Contains("Listening on ", StringComparison.Ordinal)).IsEqualTo(lifetimeInfo);
    }

    private static async Task ConfigureAsync(ProcessStartInfo start, int configuration, CancellationToken cancellationToken)
    {
        var names = new List<string>(start.Environment.Keys);
        for (var index = 0; index < names.Count; index++)
        {
            var name = names[index];
            if (name.StartsWith("Logging__", StringComparison.OrdinalIgnoreCase) || name.StartsWith("DOTNET_Logging__", StringComparison.OrdinalIgnoreCase))
                _ = start.Environment.Remove(name);
        }

        start.Environment["DOTNET_ENVIRONMENT"] = "Production";
        if (configuration is FileConfig or EnvironmentOverFileConfig)
        {
            await WriteLoggingSettingsAsync(
                start.WorkingDirectory,
                """{"LogLevel":{"Default":"Information","Squirix.Server.Adapters.Endpoint.NativeManagementEndpoint":"Information"}}""",
                cancellationToken);
        }

        if (configuration == EnvironmentFileConfig)
        {
            await WriteLoggingSettingsAsync(
                start.WorkingDirectory,
                """{"LogLevel":{"Squirix.Server.Adapters.Endpoint.NativeManagementEndpoint":"Warning"}}""",
                cancellationToken);
            await File.WriteAllTextAsync(
                Path.Join(start.WorkingDirectory, "appsettings.Production.json"),
                """{"Logging":{"LogLevel":{"Squirix.Server.Adapters.Endpoint.NativeManagementEndpoint":"Information"}}}""",
                cancellationToken);
        }

        if (configuration == EnvironmentOverFileConfig)
            start.Environment["Logging__LogLevel__Default"] = "Warning";

        if (configuration is EnvironmentConfig or EnvironmentOverFileConfig)
            start.Environment["Logging__LogLevel__Squirix.Server.Adapters.Endpoint.NativeManagementEndpoint"] = configuration == EnvironmentConfig ? "Information" : "Warning";
    }

    private static async Task WriteLoggingSettingsAsync(string directory, string loggingJson, CancellationToken cancellationToken)
    {
        var path = Path.Join(directory, "appsettings.json");
        var json = await File.ReadAllTextAsync(path, cancellationToken);
        var settings = JsonNode.Parse(json)!;
        settings["Logging"] = JsonNode.Parse(loggingJson);
        await File.WriteAllTextAsync(path, settings.ToJsonString(), cancellationToken);
    }
}
