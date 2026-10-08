using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Only network-replication-activated hosts leave expiry to committed records and run the leader expiration sweep.</summary>
public sealed class ExpiryAuthorityWiringTests : NodeIntegrationTestBase
{
    /// <summary>A foundation-only host keeps expiring on its local clock and runs no sweep.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FoundationKeepsLocalClock(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);

        await AssertWiringAsync(cluster["node-a"], CacheExpiryAuthority.LocalClock, false);
    }

    /// <summary>An RF=1 host keeps expiring on its local clock and runs no sweep.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneKeepsLocalClock(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("n1", new IntegrationStartOptions { EnableReplication = false }, cancellationToken);

        await AssertWiringAsync(cluster["n1"], CacheExpiryAuthority.LocalClock, false);
    }

    /// <summary>An activated RF=2 host leaves expiry to committed records and runs the sweep.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoUsesCommittedRecords(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())]);
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, EnableReplication = true, ExtraScope = "rf2-expiry-authority" };
        var host = await cluster.StartNodeAsync("n1", options, cancellationToken);

        await AssertWiringAsync(host, CacheExpiryAuthority.CommittedRecords, true);
    }

    private static async Task AssertWiringAsync(ITestNodeHost host, CacheExpiryAuthority expected, bool sweeps)
    {
        var sweepRegistered = false;
        foreach (var service in host.GetRequiredService<IEnumerable<IHostedService>>())
            sweepRegistered |= service is ReplicaExpirationSweepService;

        _ = await Assert.That(host.GetRequiredService<CacheExpiryAuthority>()).IsEqualTo(expected);
        _ = await Assert.That(sweepRegistered).IsEqualTo(sweeps);
    }
}
