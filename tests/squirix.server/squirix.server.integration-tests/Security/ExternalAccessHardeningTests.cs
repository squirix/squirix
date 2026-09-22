using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies external-access hardening for the primary HTTPS listener.</summary>
public sealed class ExternalAccessHardeningTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-external-hardening";

    /// <summary>Verifies health is served on the primary HTTPS listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HealthServedOnPrimaryHttpsListener(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { Security = new TestNodeSecurityOptions() }, cancellationToken);

        var response = await HttpClient.GetAsync(new Uri(cluster[NodeId].Uri, "/health"), cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>Verifies non-loopback primary listeners start when JWT authentication is configured.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonLoopbackListenWithJwtSucceeds(CancellationToken cancellationToken)
    {
        using var held = AllocateDedicatedPort();
        var uri = new UriBuilder(Uri.UriSchemeHttps, "0.0.0.0", held.Port).Uri;

        await using var cluster = await StartClusterAsync(
            new ClusterNode(NodeId, uri),
            new IntegrationStartOptions { Security = TestJwtHelper.ToSecurityOptions(TestJwtHelper.CreateRandomCredentials()) },
            cancellationToken);

        var clientUri = new UriBuilder(Uri.UriSchemeHttps, "127.0.0.1", held.Port).Uri;
        using var channel = CreateGrpcChannel(clientUri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "auth-required" }, cancellationToken: cancellationToken).ResponseAsync);
        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }

    /// <summary>Verifies non-loopback primary listeners require authentication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionUrlRequiresAuthentication(CancellationToken cancellationToken)
    {
        using var held = AllocateDedicatedPort();
        var uri = new UriBuilder(Uri.UriSchemeHttps, "0.0.0.0", held.Port).Uri;

        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<IntegrationStartOptions>>(
            StartClusterAsync(new ClusterNode(NodeId, uri), new IntegrationStartOptions { Security = new TestNodeSecurityOptions() }, cancellationToken));
        _ = await Assert.That(ex.Message).Contains("JWT", StringComparison.Ordinal);
    }
}
