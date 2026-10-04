using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Per-client backpressure limits follow the calling principal, not the node-to-node connection that forwards its requests.</summary>
public sealed class BackpressureForwardingTests : NodeIntegrationTestBase
{
    private const int Burst = 4;
    private const int MaxCalls = 20;

    /// <summary>
    /// Principals with a per-client rate limit are refused independently on the entry node, and the owner does not
    /// pool the forwarded requests of all principals into one bucket of the forwarding connection.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestsKeepPrincipalIsolation(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var backpressure = new SquirixServerBackpressureOptions { PerClientRateLimitPerSecond = 1, PerClientRateLimitBurst = Burst };
        var options = new IntegrationStartOptions
        {
            BackpressureOptions = backpressure.ToAdmissionOptions(),
            Security = TestNodeSecurityOptions.FromJwtCredentials(credentials),
            TimeProvider = new FakeTimeProvider(),
        };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "forwarded-backpressure");
        using var channel = CreateGrpcChannel(cluster["node-a"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var admittedA = await CountAdmittedAsync(client, TestJwtHelper.CreateBearerToken(credentials, subject: "tenant-a"), key, cancellationToken);
        var admittedB = await CountAdmittedAsync(client, TestJwtHelper.CreateBearerToken(credentials, subject: "tenant-b"), key, cancellationToken);

        _ = await Assert.That(admittedA).IsGreaterThan(0);
        _ = await Assert.That(admittedA).IsLessThan(MaxCalls);
        _ = await Assert.That(admittedB).IsEqualTo(admittedA);
    }

    private static async Task<int> CountAdmittedAsync(SquirixCacheService.SquirixCacheServiceClient client, string token, string key, CancellationToken cancellationToken)
    {
        var headers = new Metadata { { "authorization", $"Bearer {token}" } };
        var admitted = 0;
        while (admitted < MaxCalls)
        {
            try
            {
                _ = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, new CallOptions(headers, cancellationToken: cancellationToken));
                admitted++;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.ResourceExhausted)
            {
                break;
            }
        }

        return admitted;
    }
}
