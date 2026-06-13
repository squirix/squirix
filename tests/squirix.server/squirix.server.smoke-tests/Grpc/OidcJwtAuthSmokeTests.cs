using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Security;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.SmokeTests.Grpc;

/// <summary>
/// Thin smoke coverage for OIDC authority JWT authentication on gRPC cache RPCs.
/// </summary>
public sealed class OidcJwtAuthSmokeTests : SmokeTestBase
{
    private const string Audience = "squirix-oidc-smoke";

    /// <summary>
    /// Ensures gRPC cache RPCs accept a valid OIDC bearer token and reject missing credentials.
    /// </summary>
    /// <returns>A task representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task CacheRpcAcceptsValidOidcJwtAndRejectsMissingAuth()
    {
        await using var authority = await MockOidcAuthority.StartAsync(DefaultCancellationToken);
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = "node-oidc-auth", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: authority.ToSecurityOptions(Audience),
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new GetValueRequest { CacheName = "default", Key = "oidc-smoke" };

        var missingAuth = await Assert.ThrowsAsync<RpcException>(async () => { _ = await client.GetValueAsync(request, cancellationToken: DefaultCancellationToken); });
        Assert.Equal(StatusCode.Unauthenticated, missingAuth.StatusCode);

        var validHeaders = new Metadata { { "authorization", $"Bearer {authority.CreateBearerToken(Audience)}" } };
        var response = await client.GetValueAsync(request, new CallOptions(validHeaders, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Found);
    }
}
