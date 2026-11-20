using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.E2ETests.Fixtures.ProcessHosting;
using Squirix.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Smoke;

/// <summary>Fast external checks of the built server process through the exported SDK and HTTP endpoints.</summary>
[Property("Suite", "Smoke")]
public sealed class ServerProcessTests
{
    /// <summary>Verifies startup, public CRUD operations, health, and metrics against a separate server process.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task ServerServesCacheAndOperationalEndpoints(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(
            false,
            cancellationToken,
            static (start, _) =>
            {
                start.ArgumentList.Add("--prometheus-metrics");
                return Task.CompletedTask;
            });
        await using var client = await SquirixClient.ConnectAsync(server.Uri, cancellationToken);
        var cache = await client.GetCacheAsync<string>("process-smoke", cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);
        var value = await cache.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value).IsEqualTo("value");
        _ = await Assert.That(await cache.RemoveAsync("key", cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("key", cancellationToken)).Found).IsFalse();

        using var live = await server.HttpClient.GetAsync(new Uri(server.Uri, "/health/live"), cancellationToken);
        _ = await Assert.That(live.StatusCode).IsEqualTo(HttpStatusCode.OK);
        using var ready = await server.HttpClient.GetAsync(new Uri(server.Uri, "/health/ready"), cancellationToken);
        _ = await Assert.That(ready.StatusCode).IsEqualTo(HttpStatusCode.OK);
        using var metrics = await server.HttpClient.GetAsync(new Uri(server.Uri, "/metrics"), cancellationToken);
        _ = await Assert.That(metrics.StatusCode).IsEqualTo(HttpStatusCode.OK);
        var body = await metrics.Content.ReadAsStringAsync(cancellationToken);
        _ = await Assert.That(ContainsSuccessfulSet(body)).IsTrue();
    }

    /// <summary>Verifies that a server started with no Prometheus opt-in does not expose the scrape endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task PrometheusEndpointIsDisabledByDefault(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(false, cancellationToken);
        using var metrics = await server.HttpClient.GetAsync(new Uri(server.Uri, "/metrics"), cancellationToken);

        _ = await Assert.That(metrics.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    /// <summary>Verifies that the selected settings file supplies the path and the flag can override its enablement.</summary>
    /// <param name="enabledInSettings">Whether the settings file enables the endpoint.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    [Timeout(60_000)]
    public async Task PrometheusUsesSelectedSettingsPath(bool enabledInSettings, CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(
            false,
            cancellationToken,
            async (start, token) =>
            {
                var settingsIndex = start.ArgumentList.IndexOf("--settings") + 1;
                var settings = JsonNode.Parse(await File.ReadAllTextAsync(start.ArgumentList[settingsIndex], token))!;
                settings["Squirix"]!["PrometheusMetrics"] = new JsonObject
                {
                    ["enabled"] = enabledInSettings,
                    ["path"] = "/custom-metrics",
                };
                var customPath = Path.Join(start.WorkingDirectory, "custom.settings.json");
                await File.WriteAllTextAsync(customPath, settings.ToJsonString(), token);
                start.ArgumentList[settingsIndex] = customPath;

                if (!enabledInSettings)
                    start.ArgumentList.Add("--prometheus-metrics");
            });

        using var customMetrics = await server.HttpClient.GetAsync(new Uri(server.Uri, "/custom-metrics"), cancellationToken);
        _ = await Assert.That(customMetrics.StatusCode).IsEqualTo(HttpStatusCode.OK);
        using var defaultMetrics = await server.HttpClient.GetAsync(new Uri(server.Uri, "/metrics"), cancellationToken);
        _ = await Assert.That(defaultMetrics.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    /// <summary>Verifies that the process requires JWT for cache access while its health probes remain public.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task AuthenticatedServerRequiresJwt(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(true, cancellationToken);
        _ = await Assert.That(await ProbeAnonymousAsync(server.Uri, cancellationToken)).IsEqualTo(CacheTransportStatusCode.Unauthenticated);

        await using var client = await SquirixClient.ConnectAsync(
            options =>
            {
                options.Endpoints.Add(server.Uri);
                options.BearerTokenProvider = _ => ValueTask.FromResult(server.BearerToken!);
            },
            cancellationToken);
        var cache = await client.GetCacheAsync<string>("authenticated-smoke", cancellationToken);
        await cache.SetAsync("key", "authenticated", cancellationToken: cancellationToken);
        _ = await Assert.That((await cache.GetValueAsync("key", cancellationToken)).Value).IsEqualTo("authenticated");
        using var live = await server.HttpClient.GetAsync(new Uri(server.Uri, "/health/live"), cancellationToken);
        _ = await Assert.That(live.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    private static async Task<CacheTransportStatusCode> ProbeAnonymousAsync(Uri uri, CancellationToken cancellationToken)
    {
        try
        {
            await using var client = await SquirixClient.ConnectAsync(uri, cancellationToken);
            var cache = await client.GetCacheAsync<string>("anonymous-smoke", cancellationToken);
            _ = await cache.GetValueAsync("key", cancellationToken);
            return CacheTransportStatusCode.Ok;
        }
        catch (SquirixTransportException ex)
        {
            return ex.StatusCode;
        }
    }

    private static bool ContainsSuccessfulSet(string body)
    {
        foreach (var line in body.AsSpan().EnumerateLines())
        {
            if (!line.StartsWith("squirix_ops_total{", StringComparison.Ordinal) ||
                !line.Contains("operation=\"set\"", StringComparison.Ordinal) ||
                !line.Contains("result=\"ok\"", StringComparison.Ordinal))
                continue;

            var separator = line.LastIndexOf(' ');
            if (separator >= 0 && double.TryParse(line[(separator + 1)..], NumberStyles.Float, CultureInfo.InvariantCulture, out var value) && value > 0)
                return true;
        }

        return false;
    }
}
