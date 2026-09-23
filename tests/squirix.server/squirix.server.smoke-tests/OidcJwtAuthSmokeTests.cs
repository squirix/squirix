using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.SmokeTests;

/// <summary>Thin smoke coverage for OIDC authority JWT authentication on gRPC cache RPCs.</summary>
public sealed class OidcJwtAuthSmokeTests : SmokeTestBase
{
    private const string Audience = "squirix-oidc-smoke";

    /// <summary>Ensures gRPC cache RPCs accept a valid OIDC bearer token and reject missing credentials.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CacheRpcEnforcesOidcJwtAuth(CancellationToken cancellationToken)
    {
        await using var authority = await MockOidcAuthority.StartAsync(cancellationToken);

        await using var cluster = await StartClusterAsync(
            "node-oidc-auth",
            _ => new SmokeStartOptions { Security = authority.ToSecurityOptions(Audience) },
            cancellationToken);
        var uri = cluster["node-oidc-auth"].Uri;

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new GetValueAsyncRequest { CacheName = "default", Key = "oidc-smoke" };

        var missingAuth = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetValueAsync(request, cancellationToken: cancellationToken).ResponseAsync);
        _ = await Assert.That(missingAuth.StatusCode).IsEqualTo(StatusCode.Unauthenticated);

        var validHeaders = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken(Audience)}" } };
        var response = await client.GetValueAsync(request, new CallOptions(validHeaders, cancellationToken: cancellationToken));
        _ = await Assert.That(response.Found).IsFalse();
    }
}
