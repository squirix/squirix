using System;
using System.Threading;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.SmokeTests;

/// <summary>Smoke tests for the built-in Prometheus-compatible metrics endpoint on the server host.</summary>
public sealed class ServerMetricsSmokeTests : SmokeTestBase
{
    /// <summary>Verifies that the server host exposes <c language="csharp">/metrics</c> and that basic cache operations appear in the scrape output.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MetricsExposeCountersAfterOperations(CancellationToken cancellationToken)
    {
        var uri = GetNextHttpUri();

        await using var node = await StartNodeAsync(uri, "node_A", cancellationToken: cancellationToken);
        var cache = GetCacheApiClient(node);

        const string key = "smoke:1";
        await cache.SetEntryAsync(SmokeMutationOpIds.Default, key, BuildEntry("value", version: 1), cancellationToken);

        await Task.Delay(10, cancellationToken);

        var body = await GetWithRetryAsync(new Uri(uri, "/metrics"), TimeSpan.FromMilliseconds(50), 30, cancellationToken);
        _ = await Assert.That(string.IsNullOrWhiteSpace(body)).IsFalse();
        _ = await Assert.That(body).DoesNotContain("cache=\"", StringComparison.InvariantCulture);
        _ = await Assert.That(body).DoesNotContain("exception_type=", StringComparison.InvariantCulture);

        // Line scan instead of GeneratedRegex: NonBacktracking cannot source-generate patterns with .* / [^}]* (SYSLIB1044).
        var hasOps = ContainsMetricWithLabel(body, "squirix_ops_total{", "operation=\"set\"");
        var hasAppends = ContainsMetricWithLabel(body, "squirix_journal_appends_total{", "op=\"insert\"");
        _ = await Assert.That(hasOps || hasAppends).IsTrue();
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
