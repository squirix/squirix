using System;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>Endpoint integration tests for the built-in Prometheus-compatible metrics endpoint on the server host.</summary>
public sealed class ServerMetricsTests : ServerSurfaceTestBase
{
    /// <summary>Verifies that the server host exposes <c language="csharp">/metrics</c> and that basic cache operations appear in the scrape output.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MetricsExposeCountersAfterOperations(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node_A", static _ => new BlackBoxStartOptions { MetricsEnabled = true, PrometheusMetricsEnabled = true }, cancellationToken);
        var node = cluster["node_A"];
        using var channel = CreateBinaryChannel(node.Uri);
        var client = new CacheWireClient(channel);

        _ = await client.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = SurfaceMutationOpIds.Default,
                CacheName = "default",
                Key = "smoke:1",
                Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "value" } },
            },
            cancellationToken: cancellationToken);

        var body = await GetWithRetryAsync(new Uri(node.Uri, "/metrics"), TimeSpan.FromMilliseconds(50), 30, cancellationToken);
        _ = await Assert.That(string.IsNullOrWhiteSpace(body)).IsFalse();
        _ = await Assert.That(body).DoesNotContain("cache=\"", StringComparison.InvariantCulture);
        _ = await Assert.That(body).DoesNotContain("exception_type=", StringComparison.InvariantCulture);

        // Line scan instead of GeneratedRegex: NonBacktracking cannot source-generate patterns with .* / [^}]* (SYSLIB1044).
        _ = await Assert.That(ContainsPositiveSetCounter(body)).IsTrue();
    }

    /// <summary>Verifies that an external listener only receives measurements from the host that enables collection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MeterListenerRespectsHostOptIn(CancellationToken cancellationToken)
    {
        const string enabledCache = "external-listener-enabled";
        const string disabledCache = "external-listener-disabled";
        var enabledMeasurements = 0;
        var disabledMeasurements = 0;
        using var listener = new MeterListener
        {
            InstrumentPublished = static (instrument, collector) =>
            {
                if (string.Equals(instrument.Name, "squirix_ops_total", StringComparison.Ordinal) &&
                    string.Equals(instrument.Meter.Name, "Squirix", StringComparison.Ordinal))
                    collector.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>(
            (_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (!string.Equals(tag.Key, "cache", StringComparison.Ordinal) || tag.Value is not string name)
                        continue;

                    if (string.Equals(name, enabledCache, StringComparison.Ordinal))
                        _ = Interlocked.Increment(ref enabledMeasurements);
                    else if (string.Equals(name, disabledCache, StringComparison.Ordinal))
                        _ = Interlocked.Increment(ref disabledMeasurements);
                }
            });
        listener.Start();

        await using var enabled = await StartClusterAsync("metrics-on", static _ => new BlackBoxStartOptions { MetricsEnabled = true }, cancellationToken);
        await using var disabled = await StartClusterAsync("metrics-off", static _ => new BlackBoxStartOptions { PrometheusMetricsEnabled = true }, cancellationToken);

        await SetMetricProbeAsync(enabled["metrics-on"].Uri, enabledCache, cancellationToken);
        await SetMetricProbeAsync(disabled["metrics-off"].Uri, disabledCache, cancellationToken);

        _ = await Assert.That(Volatile.Read(ref enabledMeasurements)).IsGreaterThan(0);
        _ = await Assert.That(Volatile.Read(ref disabledMeasurements)).IsEqualTo(0);
    }

    private static async Task SetMetricProbeAsync(Uri uri, string cache, CancellationToken cancellationToken)
    {
        using var channel = CreateBinaryChannel(uri);
        var client = new CacheWireClient(channel);
        _ = await client.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = SurfaceMutationOpIds.Default,
                CacheName = cache,
                Key = "key",
                Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "value" } },
            },
            cancellationToken: cancellationToken);
    }

    /// <summary>Returns whether the scrape contains a positive counter for successful set operations.</summary>
    /// <param name="body">Prometheus scrape text.</param>
    /// <returns><see langword="true" /> when a matching line is found.</returns>
    private static bool ContainsPositiveSetCounter(string body)
    {
        foreach (var line in body.AsSpan().EnumerateLines())
        {
            if (!line.StartsWith("squirix_ops_total{", StringComparison.Ordinal) ||
                !line.Contains("operation=\"set\"", StringComparison.Ordinal) ||
                !line.Contains("result=\"ok\"", StringComparison.Ordinal))
                continue;

            var valueSeparator = line.LastIndexOf(' ');
            if (valueSeparator >= 0 &&
                double.TryParse(line[(valueSeparator + 1)..], NumberStyles.Float, CultureInfo.InvariantCulture, out var value) && value > 0)
                return true;
        }

        return false;
    }

    private async Task<string> GetWithRetryAsync(Uri metricsUrl, TimeSpan delay, int attempts, CancellationToken cancellationToken)
    {
        for (var i = 0; i < attempts; i++)
        {
            var resp = await HttpClient.GetAsync(metricsUrl, cancellationToken);
            if (resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(cancellationToken);
                if (!string.IsNullOrWhiteSpace(body))
                    return body;
            }

            await Task.Delay(delay, TimeProvider.System, cancellationToken);
        }

        var last = await HttpClient.GetAsync(metricsUrl, cancellationToken);
        var lastBody = await last.Content.ReadAsStringAsync(cancellationToken);
        throw new AssertionException($"Metrics endpoint did not return expected content. Status={last.StatusCode:D} {last.ReasonPhrase}. Body='{lastBody}'");
    }
}
