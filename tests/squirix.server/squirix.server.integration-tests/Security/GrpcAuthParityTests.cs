using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies gRPC cache authentication when JWT is enabled.</summary>
public sealed class GrpcAuthParityTests : NodeIntegrationTestBase
{
    private const string NodeId = "node-grpc-parity";

    /// <summary>Verifies gRPC rejects requests authenticated with an invalid JWT bearer token.</summary>
    [Fact]
    public async Task GrpcInvalidJwtIsRejected()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", "Bearer invalid.jwt.token" } };
        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-jwt-bad" }, new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC rejects requests without credentials when JWT auth is enabled.</summary>
    [Fact]
    public async Task GrpcMissingAuthIsRejectedWhenJwtEnabled()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-auth-missing" }, cancellationToken: DefaultCancellationToken);
        });
        Assert.Equal(StatusCode.Unauthenticated, ex.StatusCode);
    }

    /// <summary>Verifies gRPC accepts requests authenticated with a valid JWT bearer token.</summary>
    [Fact]
    public async Task GrpcValidJwtSucceeds()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var uri = GetNextHttpUri();
        await using var node = await StartNodeAsync(uri, NodeId, new NodeStartOptions { Security = TestJwtHelper.ToSecurityOptions(credentials) });

        using var channel = CreateGrpcChannel(uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var headers = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var response = await client.GetValueAsync(
            new GetValueAsyncRequest { Key = "grpc-jwt-ok", CacheName = "default" },
            new CallOptions(headers, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Found);
    }
}
