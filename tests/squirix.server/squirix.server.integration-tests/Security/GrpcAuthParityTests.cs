using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.Security;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>
/// Verifies gRPC cache authentication when JWT is enabled.
/// </summary>
public sealed class GrpcAuthParityTests : IntegrationTestBase
{
    /// <summary>Verifies gRPC rejects requests authenticated with an invalid JWT bearer token.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcInvalidJwtIsRejected()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", "Bearer invalid.jwt.token" } };
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(new GetRequest { CacheName = "default", Key = "grpc-jwt-bad" }, new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC rejects requests without credentials when JWT auth is enabled.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcMissingAuthIsRejectedWhenJwtEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetAsync(new GetRequest { CacheName = "default", Key = "grpc-auth-missing" }, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC accepts requests authenticated with a valid JWT bearer token.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task GrpcValidJwtSucceeds()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var url = GetNextHttpAddress();
        var peers = new[] { new Peer { NodeId = Guid.NewGuid().ToString("N"), Url = url } };
        await using var node = await StartNodeAsync(url, peers, security: TestJwtHelper.ToSecurityOptions(credentials));

        using var channel = CreateGrpcChannel(new Uri(url, UriKind.Absolute));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var response = await client.GetValueAsync(
            new GetValueRequest { Key = "grpc-jwt-ok", CacheName = "default" },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Found);
    }
}
