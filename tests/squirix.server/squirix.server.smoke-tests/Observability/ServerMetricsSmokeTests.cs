using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Xunit;
using Xunit.Sdk;

namespace Squirix.Server.SmokeTests.Observability;

/// <summary>
/// Smoke tests for the built-in Prometheus-compatible metrics endpoint on the server host.
/// </summary>
public sealed partial class ServerMetricsSmokeTests : SmokeTestBase
{
    /// <summary>
    /// Verifies that the server host exposes <c>/metrics</c> and that basic cache operations appear in the scrape output.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task MetricsEndpointExposesCountersAfterOperations()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node_A", Url = url } };

        await using var node = await StartNodeAsync(url, peers, cancellationToken: DefaultCancellationToken);
        var cache = GetCacheApiClient(node);

        const string key = "smoke:1";
        await cache.InsertAsync(key, BuildEntry("value", version: 1), DefaultCancellationToken);

        await Task.Delay(10, DefaultCancellationToken);

        var body = await GetWithRetryAsync(url + "/metrics", TimeSpan.FromMilliseconds(50), 30);
        Assert.False(string.IsNullOrWhiteSpace(body));
        Assert.DoesNotContain("cache=\"", body);
        Assert.DoesNotContain("exception_type=", body);

        var hasOps = OpsTotalRegex().IsMatch(body);
        var match = AppendsTotalRegex().IsMatch(body);
        Assert.True(hasOps || match, $"Expected ops or journal insert counters in metrics output. Body snippet:\n{body[..Math.Min(body.Length, 2000)]}");
    }

    [GeneratedRegex("""^squirix_journal_appends_total\{.*op="insert".*\} \d+""", RegexOptions.Multiline)]
    private static partial Regex AppendsTotalRegex();

    [GeneratedRegex("""^squirix_ops_total\{.*operation="set".*\} \d+""", RegexOptions.Multiline)]
    private static partial Regex OpsTotalRegex();

    private async Task<string> GetWithRetryAsync(string metricsUrl, TimeSpan delay, int attempts)
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

            await Task.Delay(delay, DefaultCancellationToken);
        }

        var last = await HttpClient.GetAsync(metricsUrl, DefaultCancellationToken);
        var lastBody = await last.Content.ReadAsStringAsync(DefaultCancellationToken);
        throw new XunitException($"Metrics endpoint did not return expected content. Status={(int)last.StatusCode} {last.ReasonPhrase}. Body='{lastBody}'");
    }
}
