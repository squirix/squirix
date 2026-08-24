using System;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.SmokeTests;

/// <summary>Smoke tests for health probe endpoints that remain public when JWT auth is enabled.</summary>
public sealed class ProbeSmokeTests : SmokeTestBase
{
    /// <summary>Ensures documented health probes stay reachable without JWT when auth is enabled.</summary>
    [Fact]
    public async Task HealthProbesOpenWithJwtAuthEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var uri = GetNextHttpUri();

        await using var node = await StartNodeAsync(
            uri,
            "node-health",
            new SmokeNodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            DefaultCancellationToken);

        var live = await HttpClient.GetAsync(new Uri(uri, "/health/live"), DefaultCancellationToken);
        Assert.True(live.IsSuccessStatusCode);

        var ready = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), DefaultCancellationToken);
        Assert.True(ready.IsSuccessStatusCode);
    }
}
