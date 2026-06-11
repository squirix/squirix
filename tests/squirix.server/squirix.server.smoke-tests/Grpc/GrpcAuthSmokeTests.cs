using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Security;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.SmokeTests.Grpc;

/// <summary>
/// Smoke tests verifying JWT auth on gRPC cache RPCs when credentials are configured.
/// </summary>
/// <remarks>
/// <para>Protected surfaces in core <c>Squirix.Server</c> when JWT is configured:</para>
/// <list type="bullet">
/// <item><description>gRPC <c>SquirixCacheService</c> — this class</description></item>
/// <item><description><c>/metrics</c> loopback anonymous, remote JWT — <see cref="Observability.MetricsAuthSmokeTests" /></description></item>
/// <item><description><c>/health/*</c> public — <see cref="Health.HealthProbeSmokeTests" /></description></item>
/// </list>
/// <para>Remote <c>/metrics</c> rejection without credentials is covered in integration <c>MetricsEndpointAccessTests</c>.</para>
/// </remarks>
public sealed class GrpcAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";

    /// <summary>
    /// Ensures gRPC cache RPCs reject missing and invalid JWT credentials and accept a valid bearer token.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task CacheRpcRejectsMissingAndInvalidJwtAndAcceptsValidJwtWhenConfigured()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://smoke.squirix.test", "smoke-grpc");
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-grpc-auth", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: TestJwtHelper.ToSecurityOptions(credentials),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var getRequest = new GetRequest { CacheName = "default", Key = "grpc-auth-smoke" };

        var missingAuth = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(getRequest, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.Unauthenticated, missingAuth.StatusCode);

        var invalidHeaders = new Metadata { { "authorization", $"Bearer {InvalidBearerToken}" } };
        var invalidAuth = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(getRequest, new CallOptions(invalidHeaders, cancellationToken: DefaultCancellationToken));
        });
        Assert.Equal(StatusCode.Unauthenticated, invalidAuth.StatusCode);

        var validHeaders = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var response = await client.GetValueAsync(
            new GetValueRequest { CacheName = "default", Key = "grpc-auth-smoke" },
            new CallOptions(validHeaders, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Found);
    }
}
