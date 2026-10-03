using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>A hosted node resolves a backpressure client id per caller only when a per-client limit needs it.</summary>
public sealed class BackpressureClientIdTests : NodeIntegrationTestBase
{
    /// <summary>Without a per-client limit every operation shares one client id, so no caller identity is computed per call.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultHostSharesOneClientId(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node_bp_shared", new IntegrationStartOptions(), cancellationToken);

        var resolver = cluster["node_bp_shared"].GetRequiredService<IBackpressureClientIdResolver>();

        _ = await Assert.That(resolver).IsSameReferenceAs(SharedClientIdResolver.Instance);
    }

    /// <summary>A per-client limit makes the node tell callers apart by their JWT subject or connection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PerClientLimitResolvesCallerIds(CancellationToken cancellationToken)
    {
        var admission = new AdmissionOptions { PerClientMaxInFlight = 4 };
        await using var cluster = await StartClusterAsync("node_bp_client", new IntegrationStartOptions { BackpressureOptions = admission }, cancellationToken);

        var resolver = cluster["node_bp_client"].GetRequiredService<IBackpressureClientIdResolver>();

        _ = await Assert.That(resolver).IsTypeOf<HttpContextClientIdResolver>();
    }
}
