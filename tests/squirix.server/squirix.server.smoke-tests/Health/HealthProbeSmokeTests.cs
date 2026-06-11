using System;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Security;
using Xunit;

namespace Squirix.Server.SmokeTests.Health;

/// <summary>
/// Smoke tests for health probe endpoints that remain public when JWT auth is enabled.
/// </summary>
public sealed class HealthProbeSmokeTests : SmokeTestBase
{
    /// <summary>
    /// Ensures documented health probes stay reachable without JWT when auth is enabled.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task HealthProbesRemainAccessibleWithoutJwtWhenAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-health", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var live = await HttpClient.GetAsync($"{url}/health/live", DefaultCancellationToken);
        Assert.True(live.IsSuccessStatusCode, $"Expected /health/live success, got {(int)live.StatusCode} {live.ReasonPhrase}");

        var ready = await HttpClient.GetAsync($"{url}/health/ready", DefaultCancellationToken);
        Assert.True(ready.IsSuccessStatusCode, $"Expected /health/ready success, got {(int)ready.StatusCode} {ready.ReasonPhrase}");
    }
}
