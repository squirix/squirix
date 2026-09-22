using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.SmokeTests;

/// <summary>Smoke tests for health probe endpoints that remain public when JWT auth is enabled.</summary>
public sealed class ProbeSmokeTests : SmokeTestBase
{
    /// <summary>Ensures documented health probes stay reachable without JWT when auth is enabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HealthProbesOpenWithJwtAuthEnabled(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();

        await using var cluster = await StartClusterAsync(
            "node-health",
            _ => new SmokeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) },
            cancellationToken);
        var uri = cluster["node-health"].Uri;

        var live = await HttpClient.GetAsync(new Uri(uri, "/health/live"), cancellationToken);
        _ = await Assert.That(live.IsSuccessStatusCode).IsTrue();

        var ready = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken);
        _ = await Assert.That(ready.IsSuccessStatusCode).IsTrue();
    }
}
