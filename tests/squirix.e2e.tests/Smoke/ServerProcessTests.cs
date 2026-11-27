using System;
using System.Diagnostics;
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
    /// <summary>Verifies startup, public CRUD operations and health against a separate server process.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task ServerServesCacheAndOperationalEndpoints(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(false, cancellationToken);
        await using var client = await SquirixClient.ConnectAsync(server.Uri, cancellationToken);
        var cache = await client.GetCacheAsync<string>("process-smoke", cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);
        var value = await cache.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value).IsEqualTo("value");
        _ = await Assert.That(await cache.RemoveAsync("key", cancellationToken)).IsTrue();
        _ = await Assert.That((await cache.GetValueAsync("key", cancellationToken)).Found).IsFalse();

        using var live = await server.HttpClient.GetAsync(new Uri(server.ManagementUri, "/health/live"), cancellationToken);
        _ = await Assert.That(live.StatusCode).IsEqualTo(HttpStatusCode.OK);
        using var ready = await server.HttpClient.GetAsync(new Uri(server.ManagementUri, "/health/ready"), cancellationToken);
        _ = await Assert.That(ready.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Verifies that a server started with no Prometheus opt-in does not expose the scrape endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(60_000)]
    public async Task PrometheusEndpointIsDisabledByDefault(CancellationToken cancellationToken)
    {
        await using var server = await ServerProcess.StartAsync(false, cancellationToken);
        using var metrics = await server.HttpClient.GetAsync(new Uri(server.ManagementUri, "/metrics"), cancellationToken);

        _ = await Assert.That(metrics.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
    }

    /// <summary>Verifies that both opt-ins are required and the endpoint uses the path from the selected settings file.</summary>
    /// <param name="collect">Whether to enable metrics collection.</param>
    /// <param name="export">Whether the settings file enables Prometheus export.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true, true)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Timeout(60_000)]
    public async Task PrometheusRequiresBothOptions(bool collect, bool export, CancellationToken cancellationToken)
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
                    ["enabled"] = export,
                    ["path"] = "/custom-metrics",
                };
                var customPath = Path.Join(start.WorkingDirectory, "custom.settings.json");
                await File.WriteAllTextAsync(customPath, settings.ToJsonString(), token);
                start.ArgumentList[settingsIndex] = customPath;

                if (collect)
                    start.ArgumentList.Add("--metrics");
            });

        await using var client = await SquirixClient.ConnectAsync(server.Uri, cancellationToken);
        var cache = await client.GetCacheAsync<string>("process-metrics", cancellationToken);
        await cache.SetAsync("key", "value", cancellationToken: cancellationToken);

        using var customMetrics = await server.HttpClient.GetAsync(new Uri(server.ManagementUri, "/custom-metrics"), cancellationToken);
        var expectedStatus = collect && export ? HttpStatusCode.OK : HttpStatusCode.NotFound;
        _ = await Assert.That(customMetrics.StatusCode).IsEqualTo(expectedStatus);
        if (expectedStatus == HttpStatusCode.OK)
        {
            var body = await customMetrics.Content.ReadAsStringAsync(cancellationToken);
            _ = await Assert.That(ContainsSuccessfulSet(body)).IsTrue();
        }

        using var defaultMetrics = await server.HttpClient.GetAsync(new Uri(server.ManagementUri, "/metrics"), cancellationToken);
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
        using var live = await server.HttpClient.GetAsync(new Uri(server.ManagementUri, "/health/live"), cancellationToken);
        _ = await Assert.That(live.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Verifies that journal replay keeps the same keys after a capacity eviction.</summary>
    /// <param name="tryAdd">Whether the insertion uses the SDK conditional add.</param>
    /// <param name="removeEvictedKey">Whether to remove the already evicted key through the SDK.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Timeout(60_000)]
    public async Task CapacityEvictionSurvivesCrash(bool tryAdd, bool removeEvictedKey, CancellationToken cancellationToken)
    {
        var directory = Directory.CreateTempSubdirectory("squirix-capacity-recovery-");
        try
        {
            await using (var server = await ServerProcess.StartAsync(false, cancellationToken, ConfigurePersistenceAsync))
            {
                await using var client = await SquirixClient.ConnectAsync(server.Uri, cancellationToken);
                var cache = await client.GetCacheAsync<string>("limited", cancellationToken);
                await cache.SetAsync("alpha", "first", cancellationToken: cancellationToken);
                await cache.SetAsync("beta", "second", cancellationToken: cancellationToken);
                _ = await Assert.That((await cache.GetValueAsync("alpha", cancellationToken)).Value).IsEqualTo("first");
                if (tryAdd)
                    _ = await Assert.That(await cache.TryAddAsync("gamma", "third", cancellationToken: cancellationToken)).IsTrue();
                else
                    await cache.SetAsync("gamma", "third", cancellationToken: cancellationToken);

                _ = await Assert.That((await cache.GetValueAsync("beta", cancellationToken)).Found).IsFalse();
                if (removeEvictedKey)
                    _ = await Assert.That(await cache.RemoveAsync("beta", cancellationToken)).IsFalse();

                await server.StopAsync(cancellationToken);
            }

            await using var restarted = await ServerProcess.StartAsync(false, cancellationToken, ConfigurePersistenceAsync);
            await using var restartedClient = await SquirixClient.ConnectAsync(restarted.Uri, cancellationToken);
            var recovered = await restartedClient.GetCacheAsync<string>("limited", cancellationToken);
            _ = await Assert.That((await recovered.GetValueAsync("beta", cancellationToken)).Found).IsFalse();
            _ = await Assert.That((await recovered.GetValueAsync("alpha", cancellationToken)).Value).IsEqualTo("first");
            _ = await Assert.That((await recovered.GetValueAsync("gamma", cancellationToken)).Value).IsEqualTo("third");
        }
        finally
        {
            directory.Delete(true);
        }

        Task ConfigurePersistenceAsync(ProcessStartInfo start, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            start.ArgumentList.Add("--persist");
            start.ArgumentList.Add("--data-dir");
            start.ArgumentList.Add(directory.FullName);
            start.ArgumentList.Add("--cache-eviction");
            start.ArgumentList.Add("keyCount");
            start.ArgumentList.Add("--max-keys");
            start.ArgumentList.Add("2");
            return Task.CompletedTask;
        }
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
