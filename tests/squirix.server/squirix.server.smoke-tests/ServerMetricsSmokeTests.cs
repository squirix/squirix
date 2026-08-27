using System;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace Squirix.Server.SmokeTests;

/// <summary>Smoke tests for the built-in Prometheus-compatible metrics endpoint on the server host.</summary>
public sealed class ServerMetricsSmokeTests : SmokeTestBase
{
    /// <summary>
    /// Verifies that the server host exposes <c language="csharp">/metrics</c> and that basic cache operations appear in the scrape output.
    /// </summary>
    [Fact]
    public async Task MetricsExposeCountersAfterOperations()
    {
        var uri = GetNextHttpUri();

        await using var node = await StartNodeAsync(uri, "node_A", cancellationToken: DefaultCancellationToken);
        var cache = GetCacheApiClient(node);

        const string key = "smoke:1";
        await cache.SetEntryAsync(SmokeMutationOpIds.Default, key, BuildEntry("value", version: 1), DefaultCancellationToken);

        await Task.Delay(10, DefaultCancellationToken);

        var body = await GetWithRetryAsync(new Uri(uri, "/metrics"), TimeSpan.FromMilliseconds(50), 30);
        Assert.False(string.IsNullOrWhiteSpace(body));
        Assert.DoesNotContain("cache=\"", body, StringComparison.InvariantCulture);
        Assert.DoesNotContain("exception_type=", body, StringComparison.InvariantCulture);

        // Line scan instead of GeneratedRegex: NonBacktracking cannot source-generate patterns with .* / [^}]* (SYSLIB1044).
        var hasOps = ContainsMetricWithLabel(body, "squirix_ops_total{", "operation=\"set\"");
        var hasAppends = ContainsMetricWithLabel(body, "squirix_journal_appends_total{", "op=\"insert\"");
        Assert.True(hasOps || hasAppends);
    }

    /// <summary>Returns whether any scrape line starts with <paramref name="metricPrefix" /> and contains <paramref name="label" />.</summary>
    /// <param name="body">Prometheus scrape text.</param>
    /// <param name="metricPrefix">Metric name prefix including the opening brace.</param>
    /// <param name="label">Required label fragment inside the metric line.</param>
    /// <returns><see langword="true" /> when a matching line is found.</returns>
    private static bool ContainsMetricWithLabel(string body, string metricPrefix, string label)
    {
        var remaining = body.AsSpan();
        while (!remaining.IsEmpty)
        {
            var eol = remaining.IndexOfAny('\r', '\n');
            var line = eol < 0 ? remaining : remaining[..eol];
            if (eol < 0)
            {
                remaining = [];
            }
            else
            {
                var skip = eol + 1;
                if (remaining[eol] == '\r' && skip < remaining.Length && remaining[skip] == '\n')
                    skip++;
                remaining = remaining[skip..];
            }

            if (line.StartsWith(metricPrefix, StringComparison.Ordinal) && line.Contains(label, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private async Task<string> GetWithRetryAsync(Uri metricsUrl, TimeSpan delay, int attempts)
    {
        for (var i = 0; i < attempts; i++)
        {
            var resp = await HttpClient.GetAsync(metricsUrl, DefaultCancellationToken);
            if (resp.IsSuccessStatusCode)
            {
                var body = await resp.Content.ReadAsStringAsync(DefaultCancellationToken);
                if (!string.IsNullOrWhiteSpace(body))
                    return body;
            }

            await Task.Delay(delay, TimeProvider.System, DefaultCancellationToken);
        }

        var last = await HttpClient.GetAsync(metricsUrl, DefaultCancellationToken);
        var lastBody = await last.Content.ReadAsStringAsync(DefaultCancellationToken);
        throw new XunitException($"Metrics endpoint did not return expected content. Status={last.StatusCode:D} {last.ReasonPhrase}. Body='{lastBody}'");
    }
}
