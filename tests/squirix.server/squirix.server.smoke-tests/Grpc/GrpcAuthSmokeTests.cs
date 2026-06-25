using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.SmokeTests.Support;
using Squirix.Server.TestKit.Auth;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.SmokeTests.Grpc;

/// <summary>Smoke tests verifying JWT auth on gRPC cache RPCs when credentials are configured.</summary>
public sealed class GrpcAuthSmokeTests : SmokeTestBase
{
    private const string InvalidBearerToken = "invalid.jwt.token";

    /// <summary>Ensures gRPC cache RPCs reject missing and invalid JWT credentials and accept a valid bearer token.</summary>
    [Fact]
    public async Task CacheRpcRejectsMissingAndInvalidJwtAndAcceptsValidJwtWhenConfigured()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://smoke.squirix.test", "smoke-grpc");
        var url = GetNextHttpUri();

        await using var node = await StartNodeAsync(
            url,
            "node-grpc-auth",
            security: TestJwtHelper.ToSecurityOptions(credentials),
            cancellationToken: DefaultCancellationToken);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var getRequest = new GetEntryAsyncRequest { CacheName = "default", Key = "grpc-auth-smoke" };

        var missingAuth = await Assert.ThrowsAsync<RpcException>(() => client.GetEntryAsync(getRequest, cancellationToken: DefaultCancellationToken).ResponseAsync);
        Assert.Equal(StatusCode.Unauthenticated, missingAuth.StatusCode);

        var invalidHeaders = new Metadata { { "authorization", $"Bearer {InvalidBearerToken}" } };
        var invalidAuth = await Assert.ThrowsAsync<RpcException>(() =>
            client.GetEntryAsync(getRequest, new CallOptions(invalidHeaders, cancellationToken: DefaultCancellationToken)).ResponseAsync);
        Assert.Equal(StatusCode.Unauthenticated, invalidAuth.StatusCode);

        var validHeaders = new Metadata { { "authorization", $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}" } };
        var response = await client.GetValueAsync(
            new GetValueAsyncRequest { CacheName = "default", Key = "grpc-auth-smoke" },
            new CallOptions(validHeaders, cancellationToken: DefaultCancellationToken));
        Assert.False(response.Found);
    }
}
