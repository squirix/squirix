using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.SmokeTests.Support;
using Squirix.Server.TestKit.Auth;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.SmokeTests.Grpc;

/// <summary>Thin smoke coverage for OIDC authority JWT authentication on gRPC cache RPCs.</summary>
public sealed class OidcJwtAuthSmokeTests : SmokeTestBase
{
    private const string Audience = "squirix-oidc-smoke";

    /// <summary>Ensures gRPC cache RPCs accept a valid OIDC bearer token and reject missing credentials.</summary>
    [Fact]
    public async Task CacheRpcAcceptsValidOidcJwtAndRejectsMissingAuth()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var uri = GetNextHttpUri();

        await using var node = await StartNodeAsync(
            uri,
            "node-oidc-auth",
            security: authority.ToSecurityOptions(Audience),
            cancellationToken: DefaultCancellationToken);

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new GetValueAsyncRequest { CacheName = "default", Key = "oidc-smoke" };

        var missingAuth = await Assert.ThrowsAsync<RpcException>(() => client.GetValueAsync(request, cancellationToken: DefaultCancellationToken).ResponseAsync);
        Assert.Equal(StatusCode.Unauthenticated, missingAuth.StatusCode);

        var validHeaders = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken(Audience)}" } };
        var response = await client.GetValueAsync(request, new CallOptions(validHeaders, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Found);
    }
}
